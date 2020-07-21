#include "transport/backend/Thallium/EndpointGroup.hpp"

#include <memory>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "transport/backend/Thallium/Utilities.hpp"
#include "utils/type_traits.hpp"
#include "utils/macros.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace Thallium {

EndpointGroup::EndpointGroup(const Thallium::Engine_t &engine,
                             const Thallium::RPC_t &process_rpc,
                             const Thallium::RPC_t &cleanup_rpc)
  : ::Transport::EndpointGroup(),
    engine(engine),
    process_rpc(process_rpc),
    cleanup_rpc(cleanup_rpc),
    endpoints()
{
    if (!process_rpc) {
        throw std::runtime_error("Bad RPC for processing requests");
    }
    if (!cleanup_rpc) {
        throw std::runtime_error("Bad RPC for cleaning up responses");
    }
}

EndpointGroup::~EndpointGroup() {}

/**
 * AddID
 * Add a mapping from a unique ID to a thalllium address
 * ThalliumEndpointGroup takes ownership of ep
 *     - If the engine is null or if the lookup fails,
 *       nothing is done and TRANSPORT_ERROR is returned
 *
 * @param id       the unique ID associated with the rank
 * @param engine   the engine instance
 * @param address  the address of the other end
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
int EndpointGroup::AddID(const int id, const Thallium::Engine_t &engine, const std::string &address) {
    if (!engine) {
        return TRANSPORT_ERROR;
    }

    Thallium::Endpoint_t ep(new thallium::endpoint(engine->lookup(address)));
    if (!ep) {
        return TRANSPORT_ERROR;
    }

    return AddID(id, ep);
}

/**
 * AddID
 * Add a mapping from a unique ID to a MPI rank
 * ThalliumEndpointGroup takes ownership of ep
 *     - If ep is null, nothing is done
 *       and TRANSPORT_ERROR is returned
 *     - If there is already an endpoint at id,
 *       the endpoint is destroyed
 *
 * @param id the unique ID associated with the rank
 * @param ep the other end
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
int EndpointGroup::AddID(const int id, const Thallium::Endpoint_t &ep) {
    if (!ep) {
        return TRANSPORT_ERROR;
    }

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
void EndpointGroup::RemoveID(const int id) {
    endpoints.erase(id);
}

/**
 * do_operation
 * Does the client side of a remote send/receive operation
 * All arguments after messages are members of Thallium::EndpointGroup
 *
 * @tparam Recv_t      Recieve type; listed first to allow for Send_t to be deduced by the compiler
 * @tparam Send_t      Send type
 * @tparam (unnamed)   test to make sure the rest of the template makes sense
 * @param messages     the list of messages to send
 * @param engine       the thallium engine
 * @param rpc          the thallium rpc
 * @param buffer_size  the size of the buffer allocated for sending and receiving packets
 * @param endpoints    list of endpoints
 * @return The list of responses received
 */
template <typename Recv_t, typename Send_t, typename = enable_if_t<std::is_base_of<Request::Request,   Send_t>::value &&
                                                                   std::is_base_of<Response::Response, Recv_t>::value> >
Recv_t *do_operation(const std::unordered_map<int, Send_t *> &messages,
                     Engine_t engine,
                     RPC_t process_rpc,
                     RPC_t cleanup_rpc,
                     std::unordered_map<int, Endpoint_t> &endpoints) {

    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    Recv_t *head = nullptr;
    Recv_t *tail = nullptr;

    mlog(THALLIUM_INFO, "Sending requests in %zu buffers", messages.size());

    for(REF(messages)::value_type const &message : messages) {
        Send_t *req = message.second;

        if (!req) {
            mlog(THALLIUM_WARN, "Request going to %d does not exist", message.first);
            continue;
        }

        mlog(THALLIUM_DBG, "Request is going to range server %d", req->dst);

        // figure out where to send the message
        std::unordered_map<int, Endpoint_t>::const_iterator dst_it = endpoints.find(req->dst);
        if (dst_it == endpoints.end()) {
            mlog(THALLIUM_WARN, "Could not find endpoint for destination rank %d", req->dst);
            continue;
        }

        mlog(THALLIUM_DBG, "Packing request going to range server %d", req->dst);

        // pack the request
        clock_gettime(CLOCK_MONOTONIC, &req->timestamps.transport.pack.start);
        void *req_buf = nullptr;
        std::size_t req_size = 0;
        if (Packer::pack(req, &req_buf, &req_size) != TRANSPORT_SUCCESS) {
            mlog(THALLIUM_WARN, "Unable to pack message");
            dealloc(req_buf);
            continue;
        }
        clock_gettime(CLOCK_MONOTONIC, &req->timestamps.transport.pack.end);

        mlog(THALLIUM_DBG, "Sending packed request (%zu bytes) to %d", req_size, req->dst);

        // expose req_buf through bulk
        std::vector<std::pair<void *, std::size_t> > req_segments = {std::make_pair(req_buf, req_size)};
        thallium::bulk req_bulk = engine->expose(req_segments, thallium::bulk_mode::read_write);

        // send request_size and request
        // get back packed response_size, response, and remote address
        clock_gettime(CLOCK_MONOTONIC, &req->timestamps.transport.send_start); // store the value in req for now
        thallium::packed_response packed_res = process_rpc->on(*dst_it->second)(req_size, req_bulk);
        clock_gettime(CLOCK_MONOTONIC, &req->timestamps.transport.recv_end);   // store the value in req for now

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
        res_bulk.on(*dst_it->second) >> local;

        // unpack the response
        mlog(THALLIUM_DBG, "Unpacking %zu byte response from %d", res_size, req->dst);

        clock_gettime(CLOCK_MONOTONIC, &req->timestamps.transport.unpack.start); // store the value in req for now
        Recv_t *response = nullptr;
        const int unpack_rc = Unpacker::unpack(&response, res_buf, res_size);
        dealloc(res_buf);
        clock_gettime(CLOCK_MONOTONIC, &req->timestamps.transport.unpack.end); // store the value in req for now

        // clean up server pointer before handling any errors
        cleanup_rpc->on(*dst_it->second)(res_ptr);

        if (unpack_rc != TRANSPORT_SUCCESS) {
            mlog(THALLIUM_WARN, "Unable to unpack message");
            continue;
        }

        mlog(THALLIUM_DBG, "Unpacked %zu byte response from %d", res_size, req->dst);

        // shallow copy timestamps into response
        // response takes ownership
        response->timestamps = req->timestamps;
        req->timestamps.reqs = nullptr;

        if (!response) {
            continue;
        }

        if (!head) {
            head = response;
            tail = response;
        }
        else {
            tail->next = response;
            tail = response;
        }

        mlog(THALLIUM_DBG, "Done sending request to %d", req->dst);
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
Response::BPut *EndpointGroup::communicate(const std::unordered_map<int, Request::BPut *> &bpm_list) {
    return do_operation<Response::BPut>(bpm_list, engine, process_rpc, cleanup_rpc, endpoints);
}

/**
 * BGet
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BGet *EndpointGroup::communicate(const std::unordered_map<int, Request::BGet *> &bgm_list) {
    return do_operation<Response::BGet>(bgm_list, engine, process_rpc, cleanup_rpc, endpoints);
}

/**
 * BGetOp
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGETOP messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BGetOp *EndpointGroup::communicate(const std::unordered_map<int, Request::BGetOp *> &bgm_list) {
    return do_operation<Response::BGetOp>(bgm_list, engine, process_rpc, cleanup_rpc, endpoints);
}

/**
 * BDelete
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BDelete *EndpointGroup::communicate(const std::unordered_map<int, Request::BDelete *> &bdm_list) {
    return do_operation<Response::BDelete>(bdm_list, engine, process_rpc, cleanup_rpc, endpoints);
}

}
}
