#if HXHIM_HAVE_THALLIUM

#include "transport/Thallium/EndpointGroup.hpp"

#include <memory>
#include <unordered_map>
#include <vector>

#include "transport/Thallium/Utilities.hpp"
#include "utils/enable_if_t.hpp"
#include "utils/macros.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace Thallium {

EndpointGroup::EndpointGroup(const Thallium::Engine_t &engine,
                             const Thallium::RPC_t &rpc,
                             const std::size_t buffer_size)
  : ::Transport::EndpointGroup(),
    engine(engine),
    rpc(rpc),
    buffer_size(buffer_size),
    endpoints()
{
    if (!rpc) {
        throw std::runtime_error("thallium::remote_procedure in Thallium Endpoint Group must not be nullptr");
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
 * @tparam Recv_t    Recieve type; listed first to allow for Send_t to be deduced by the compiler
 * @tparam Send_t    Send type
 * @tparam (unnamed) test to make sure the rest of the template makes sense
 * @param messages   the list of messages to send
 * @param engine     the thallium engine
 * @param rpc        the thallium rpc
 * @param endpoints  list of endpoints
 * @param packed     memory pool to allocate packed messages from
 * @param responses  memory pool to allocate responses from
 * @param arrays     memory pool to allocate arrays from
 * @param buffers    memory pool to allocate buffers from
 * @return The list of responses received
 */
template <typename Recv_t, typename Send_t>
Recv_t *do_operation(const std::unordered_map<int, Send_t *> &messages,
                     Engine_t engine,
                     RPC_t rpc,
                     const std::size_t buffer_size,
                     std::unordered_map<int, Endpoint_t> &endpoints) {
    Recv_t *head = nullptr;
    Recv_t *tail = nullptr;

    mlog(THALLIUM_DBG, "Processing Messages in %zu buffers", messages.size());

    // buffer used for rdma
    void *buf = alloc(buffer_size);
    memset(buf, 0, buffer_size);

    for(REF(messages)::value_type const &message : messages) {
        Send_t *req = message.second;

        if (!req) {
            mlog(THALLIUM_WARN, "Message going to %d does not exist", message.first);
            continue;
        }

        mlog(THALLIUM_DBG, "Message is going to range server %d", req->dst);

        // figure out where to send the message
        std::unordered_map<int, Endpoint_t>::const_iterator dst_it = endpoints.find(req->dst);

        if (dst_it == endpoints.end()) {
            mlog(THALLIUM_WARN, "Could not find endpoint for destination rank %d", req->dst);
            continue;
        }

        // pack the request
        if (req->pack(buf, buffer_size) != TRANSPORT_SUCCESS) {
            mlog(THALLIUM_WARN, "Unable to pack message");
            continue;
        }

        mlog(THALLIUM_WARN, "Sending message to %d packed into a buffer of size %zu", req->dst, buffer_size);

        // create the bulk message, send it, and get the size in response the response data is in buf
        std::vector<std::pair<void *, std::size_t> > segments = {std::make_pair(buf, buffer_size)};
        thallium::bulk bulk = engine->expose(segments, thallium::bulk_mode::read_write);
        const std::size_t response_size = rpc->on(*dst_it->second)(bulk);

        mlog(THALLIUM_DBG, "Received %zu byte response from %d", response_size, req->dst);

        // unpack the response
        mlog(THALLIUM_WARN, "Unpacking %zu byte response from %d", response_size, req->dst);

        Recv_t *response = nullptr;
        response = construct <Recv_t>(buf, buffer_size);
        // if (Unpacker::unpack(&response, buf, response_size) != TRANSPORT_SUCCESS) {
        //     mlog(THALLIUM_WARN, "Unable to unpack message");
        //     continue;
        // }

        mlog(THALLIUM_DBG, "Unpacked %zu byte response from %d", response_size, req->dst);

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

        mlog(THALLIUM_DBG, "Done processing message to %d", req->dst);
    }

    dealloc(buf);

    mlog(THALLIUM_DBG, "Done processing messages");

    return head;
}

/**
 * BPut
 *
 * @param num_rangesrvs the total number of range servers
 * @param bpm_list the list of BPUT messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::RecvBPut *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBPut *> &bpm_list) {
    return do_operation<Response::RecvBPut>(bpm_list, engine, rpc, buffer_size, endpoints);
}

/**
 * BGet
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::RecvBGet *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBGet *> &bgm_list) {
    return do_operation<Response::RecvBGet>(bgm_list, engine, rpc, buffer_size, endpoints);
}

/**
 * BGetOp
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGETOP messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::RecvBGetOp *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBGetOp *> &bgm_list) {
    return do_operation<Response::RecvBGetOp>(bgm_list, engine, rpc, buffer_size, endpoints);
}

/**
 * BDelete
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::RecvBDelete *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBDelete *> &bdm_list) {
    return do_operation<Response::RecvBDelete>(bdm_list, engine, rpc, buffer_size, endpoints);
}

// /**
//  * BHistogram
//  *
//  * @param num_rangesrvs the total number of range servers
//  * @param bdm_list the list of BDELETE messages to send
//  * @return a linked list of response messages, or nullptr
//  */
// Response::RecvBHistogram *EndpointGroup::communicate(const std::unordered_map<int, Request::SendBHistogram *> &bhist_list) {
//     return do_operation<Response::RecvBHistogram>(bhist_list, engine, rpc, buffer_size, endpoints);
// }

}
}

#endif
