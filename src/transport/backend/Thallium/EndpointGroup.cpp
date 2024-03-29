#ifdef ASYNC_THALLIUM
#include <future>
#include <list>
#endif

#include <unordered_map>
#include <utility>
#include <vector>

#include "transport/backend/Thallium/EndpointGroup.hpp"
#include "transport/backend/Thallium/Utilities.hpp"
#include "utils/macros.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"
#include "utils/type_traits.hpp"

Transport::Thallium::EndpointGroup::EndpointGroup(thallium::engine *engine,
                                                  RangeServer *rs)
    : ::Transport::EndpointGroup(),
      engine(engine),
      rs(rs),
      endpoints()
{}

Transport::Thallium::EndpointGroup::~EndpointGroup() {
    for(decltype(endpoints)::value_type const ep : endpoints) {
        destruct(ep.second);
    }
    engine->finalize();
    destruct(engine);
}

/**
 * AddID
 * Add a mapping from a unique ID to a thalllium address.
 *
 * @param id       the unique ID associated with the rank
 * @param address  the address of the other end
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
int Transport::Thallium::EndpointGroup::AddID(const int id, const std::string &address) {
    return AddID(id, construct<thallium::endpoint>(engine->lookup(address)));
}

/**
 * AddID
 * Add a mapping from a unique ID to a MPI rank
 * Thallium::EndpointGroup takes ownership of ep
 *     - If ep is null, nothing is done
 *       and TRANSPORT_ERROR is returned
 *     - If there is already an endpoint at id,
 *       the endpoint is destroyed
 *
 * @param id the unique ID associated with the rank
 * @param ep the other end
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
int Transport::Thallium::EndpointGroup::AddID(const int id, thallium::endpoint *ep) {
    if (!ep) {
        return TRANSPORT_ERROR;
    }

    RemoveID(id);
    endpoints[id] = ep;
    return TRANSPORT_SUCCESS;
}

/**
 * RemoveID
 * Remove a mapping from a unique ID to a rank
 * If the unique ID is not found, nothing is done
 *
 * @param id the unique ID to remove
 */
void Transport::Thallium::EndpointGroup::RemoveID(const int id) {
    decltype(endpoints)::const_iterator rm = endpoints.find(id);
    if (rm != endpoints.end()) {
        destruct(rm->second);
        endpoints.erase(rm);
    }
}

/**
 * process request
 * Does the client side of a single remote send/receive operation
 * All arguments after messages are members of Thallium::EndpointGroup
 *
 * @tparam Recv_t      Receive type; listed first to allow for Send_t to be deduced by the compiler
 * @tparam Send_t      Send type
 * @tparam (unnamed)   test to make sure the rest of the template makes sense
 * @param messages     the list of messages to send
 * @param engine       the thallium engine
 * @param rpc          the thallium rpc
 * @param buffer_size  the size of the buffer allocated for sending and receiving packets
 * @param endpoints    the endpoint to send to
 * @return The list of responses received
 */
template <typename Recv_t, typename Send_t,
          typename = enable_if_t<std::is_base_of<Message::Request::Request,   Send_t>::value &&
                                 std::is_base_of<Message::Response::Response, Recv_t>::value> >
inline Recv_t *process_request(Send_t *req,
                               thallium::engine *engine,
                               Transport::Thallium::RangeServer *rs,
                               const std::unordered_map<int, thallium::endpoint *> &endpoints) {
    // if (!req || !engine || !rs || !endpoint) {
    //     return nullptr;
    // }

    // figure out where to send the message
    REF(endpoints)::const_iterator dst_it = endpoints.find(req->dst_rank);
    if (dst_it == endpoints.end()) {
        mlog(THALLIUM_WARN, "Could not find endpoint for destination rank %d", req->dst);
        return nullptr;
    }

    mlog(THALLIUM_DBG, "Request is going to range server %d", req->dst);

    req->timestamps.transport.start = ::Stats::now();

    mlog(THALLIUM_DBG, "Packing request going to range server %d", req->dst);

    // pack the request
    req->timestamps.transport.pack.start = ::Stats::now();
    void *req_buf = nullptr;
    std::size_t req_size = 0;
    if (Message::Packer::pack(req, &req_buf, &req_size) != MESSAGE_SUCCESS) {
        mlog(THALLIUM_WARN, "Unable to pack message");
        dealloc(req_buf);
        return nullptr;
    }
    req->timestamps.transport.pack.end = ::Stats::now();

    mlog(THALLIUM_DBG, "Sending packed request (%zu bytes) to %d", req_size, req->dst);

    // expose req_buf through bulk
    std::vector<std::pair<void *, std::size_t> > req_segments = {std::make_pair(req_buf, req_size)};
    thallium::bulk req_bulk = engine->expose(req_segments, thallium::bulk_mode::read_write);

    // send request_size and request
    // get back packed response_size, response, and remote address
    req->timestamps.transport.send_start = ::Stats::now(); // store the value in req for now
    thallium::packed_response packed_res = rs->process().on(*(dst_it->second))(req_size, req_bulk);
    req->timestamps.transport.recv_end = ::Stats::now();   // store the value in req for now

    dealloc(req_buf);

    // unpack thallium::packed_response
    std::size_t res_size;
    thallium::bulk res_bulk;
    uintptr_t res_ptr;
    std::tie(res_size, res_bulk, res_ptr) = packed_res.as<std::size_t, thallium::bulk, uintptr_t> ();

    mlog(THALLIUM_DBG, "Received %zu byte response from %d", res_size, req->dst);

    // read the data out of the response bulk
    void *res_buf = alloc(res_size);
    std::vector<std::pair<void *, std::size_t> > res_segments = {
        std::make_pair((void *) res_buf, res_size)};
    thallium::bulk local = engine->expose(res_segments, thallium::bulk_mode::write_only);
    res_bulk.on(*(dst_it->second)) >> local;

    // unpack the response
    mlog(THALLIUM_DBG, "Unpacking %zu byte response from %d", res_size, req->dst);

    req->timestamps.transport.unpack.start = ::Stats::now(); // store the value in req for now
    Recv_t *response = nullptr;
    const int unpack_rc = Message::Unpacker::unpack(&response, res_buf, res_size);
    dealloc(res_buf);
    req->timestamps.transport.unpack.end = ::Stats::now(); // store the value in req for now

    // clean up server pointer before handling any errors
    req->timestamps.transport.cleanup_rpc.start = ::Stats::now(); // store the value in req for now
    rs->cleanup().on(*(dst_it->second))(res_ptr);
    req->timestamps.transport.cleanup_rpc.end = ::Stats::now(); // store the value in req for now

    if (unpack_rc != MESSAGE_SUCCESS) {
        mlog(THALLIUM_WARN, "Unable to unpack message");
        return nullptr;
    }

    mlog(THALLIUM_DBG, "Unpacked %zu byte response from %d", res_size, req->dst);

    req->timestamps.transport.end = ::Stats::now();

    if (!response) {
        return nullptr;
    }

    response->steal_timestamps(req, true);

    return response;
}

/**
 * process requests
 * Sends off all requests provided
 * All arguments after messages are members of Thallium::EndpointGroup
 *
 * @tparam Recv_t      Receive type; listed first to allow for Send_t to be deduced by the compiler
 * @tparam Send_t      Send type
 * @tparam (unnamed)   test to make sure the rest of the template makes sense
 * @param messages     the list of messages to send
 * @param engine       the thallium engine
 * @param rpc          the thallium rpc
 * @param buffer_size  the size of the buffer allocated for sending and receiving packets
 * @param endpoints    list of endpoints
 * @return The list of responses received
 */
template <typename Recv_t, typename Send_t,
          typename = enable_if_t<std::is_base_of<Message::Request::Request,   Send_t>::value &&
                                 std::is_base_of<Message::Response::Response, Recv_t>::value> >
Recv_t *process_requests(const Transport::ReqList<Send_t> &messages,
                         thallium::engine *engine,
                         Transport::Thallium::RangeServer *rs,
                         const std::unordered_map<int, thallium::endpoint *> &endpoints) {
    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    mlog(THALLIUM_INFO, "Sending %zu requests", messages.size());

    #ifdef ASYNC_THALLIUM
    std::list<std::future<Recv_t *> > futures;
    #endif

    Recv_t *head = nullptr;
    Recv_t *tail = nullptr;
    for(REF(messages)::value_type const &message : messages) {
        Send_t *req = message.second;
        if (!req) {
            mlog(THALLIUM_WARN, "Request going to %d does not exist", message.first);
            continue;
        }

        mlog(THALLIUM_DBG, "Sending request to %d", req->dst);

    #ifdef ASYNC_THALLIUM
        futures.emplace_back(std::async(std::launch::async,
                                        process_request<Recv_t, Send_t>,
                                        req, engine, rs, std::cref(endpoints)));

        mlog(THALLIUM_DBG, "Done sending request to %d", req->dst);
    }

    for(std::future<Recv_t *> &future : futures) {
        Recv_t *response = future.get();
    #else
        Recv_t *response = process_request<Recv_t, Send_t>(req, engine, rs, endpoints);
    #endif

        mlog(THALLIUM_DBG, "Received response from %d", response->src);
        if (response) {
            if (!head) {
                head = response;
            }
            else {
                tail->next = response;
            }
            tail = response;
        }
    }

    mlog(THALLIUM_INFO, "Done sending requests and receiving responses");
    return head;
}

/**
 * BPut
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list the list of BPUT messages to send
 * @return a linked list of response messages, or nullptr
 */
Message::Response::BPut *Transport::Thallium::EndpointGroup::communicate(const ReqList<Message::Request::BPut> &bpm_list) {
    return process_requests<Message::Response::BPut>(bpm_list, engine, rs, endpoints);
}

/**
 * BGet
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Message::Response::BGet *Transport::Thallium::EndpointGroup::communicate(const ReqList<Message::Request::BGet> &bgm_list) {
    return process_requests<Message::Response::BGet>(bgm_list, engine, rs, endpoints);
}

/**
 * BGetOp
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGETOP messages to send
 * @return a linked list of response messages, or nullptr
 */
Message::Response::BGetOp *Transport::Thallium::EndpointGroup::communicate(const ReqList<Message::Request::BGetOp> &bgm_list) {
    return process_requests<Message::Response::BGetOp>(bgm_list, engine, rs, endpoints);
}

/**
 * BDelete
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Message::Response::BDelete *Transport::Thallium::EndpointGroup::communicate(const ReqList<Message::Request::BDelete> &bdm_list) {
    return process_requests<Message::Response::BDelete>(bdm_list, engine, rs, endpoints);
}

/**
 * BHistogram
 *
 * @param num_rangesrvs the total number of range servers
 * @param bhm_list the list of BHISTOGRAM messages to send
 * @return a linked list of response messages, or nullptr
 */
Message::Response::BHistogram *Transport::Thallium::EndpointGroup::communicate(const ReqList<Message::Request::BHistogram> &bhm_list) {
    return process_requests<Message::Response::BHistogram>(bhm_list, engine, rs, endpoints);
}
