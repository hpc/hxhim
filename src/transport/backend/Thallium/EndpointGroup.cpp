#if HXHIM_HAVE_THALLIUM

#include "transport/backend/Thallium/EndpointGroup.hpp"

#include <memory>
#include <unordered_map>
#include <vector>

#include <thallium/serialization/stl/string.hpp>

#include "transport/backend/Thallium/Endpoint.hpp"
#include "transport/backend/Thallium/Packer.hpp"
#include "transport/backend/Thallium/Unpacker.hpp"
#include "transport/backend/Thallium/Utilities.hpp"
#include "utils/enable_if_t.hpp"
#include "utils/macros.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace Thallium {

EndpointGroup::EndpointGroup(const Thallium::Engine_t &engine,
                                                  const Thallium::RPC_t &rpc,
                                                  FixedBufferPool *packed,
                                                  FixedBufferPool *responses,
                                                  FixedBufferPool *arrays,
                                                  FixedBufferPool *buffers)
  : ::Transport::EndpointGroup(),
    engine(engine),
    rpc(rpc),
    endpoints(),
    packed(packed),
    responses(responses),
    arrays(arrays),
    buffers(buffers)
{
    if (!rpc) {
        throw std::runtime_error("thallium::remote_procedure in Thallium Endpoint Group must not be nullptr");
    }

    if (!packed) {
        throw std::runtime_error("FixedBufferPool in Thallium Endpoint Group must not be nullptr");
    }

    if (!responses) {
        throw std::runtime_error("FixedBufferPool in Thallium Endpoint Group must not be nullptr");
    }

    if (!arrays) {
        throw std::runtime_error("FixedBufferPool in Thallium Endpoint Group must not be nullptr");
    }

    if (!buffers) {
        throw std::runtime_error("FixedBufferPool in Thallium Endpoint Group must not be nullptr");
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
template <typename Recv_t, typename Send_t, typename = enable_if_t<std::is_base_of<Request::Request,   Send_t>::value &&
                                                                   std::is_base_of<Bulk,               Send_t>::value &&
                                                                   std::is_base_of<Response::Response, Recv_t>::value &&
                                                                   std::is_base_of<Bulk,               Recv_t>::value> >
Recv_t *do_operation(const std::unordered_map<int, Send_t *> &messages,
                     Engine_t engine,
                     RPC_t rpc,
                     std::unordered_map<int, Endpoint_t> &endpoints,
                     FixedBufferPool *packed,
                     FixedBufferPool *responses,
                     FixedBufferPool *arrays,
                     FixedBufferPool *buffers) {
    Recv_t *head = nullptr;
    Recv_t *tail = nullptr;

    // mlog(THALLIUM_DBG, "Processing Messages in %zu buffers", messages.size());

    for(REF(messages)::value_type const &message : messages) {
        Send_t *msg = message.second;

        if (!msg) {
            mlog(THALLIUM_WARN, "Message going to %d does not exist", message.first);
            continue;
        }

        mlog(THALLIUM_DBG, "Message is going to range server %d", msg->dst);

        // figure out where to send the message
        std::unordered_map<int, Endpoint_t>::const_iterator dst_it = endpoints.find(msg->dst);

        if (dst_it == endpoints.end()) {
            // mlog(THALLIUM_ERR, "Could not find endpoint for destination rank %d", msg->dst);
            continue;
        }

        // pack the request
        // doing some bad stuff here: the actual allocated buffer should be large enough to allow for responses
        void *buf = nullptr;
        std::size_t bufsize = 0;
        if (Packer::pack(msg, &buf, &bufsize, packed) != TRANSPORT_SUCCESS) {
            // mlog(THALLIUM_DBG, "Unable to pack message");
            continue;
        }

        // mlog(THALLIUM_DBG, "Sending message to %d packed into a buffer of size %zu", msg->dst, bufsize);

        // create the bulk message, send it, and get the size in response the response data is in buf
        std::vector<std::pair<void *, std::size_t> > segments = {std::make_pair(buf, packed->alloc_size())};
        thallium::bulk bulk = engine->expose(segments, thallium::bulk_mode::read_write);
        const std::size_t response_size = rpc->on(*dst_it->second)(bulk);
        // mlog(THALLIUM_DBG, "Received %zu byte response from %d", response_size, msg->dst);

        // unpack the response
        Recv_t *response = nullptr;
        const int ret = Unpacker::unpack(&response, buf, response_size, responses, arrays, buffers);
        packed->release(buf, bufsize);
        if (ret != TRANSPORT_SUCCESS) {
            // mlog(THALLIUM_DBG, "Unable to unpack message");
            continue;
        }

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

        // mlog(THALLIUM_DBG, "Done processing message to %d", msg->dst);
    }

    // mlog(THALLIUM_DBG, "Done processing messages");

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
    return do_operation<Response::BPut>(bpm_list, engine, rpc, endpoints, packed, responses, arrays, buffers);
}

/**
 * BGet
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BGet *EndpointGroup::communicate(const std::unordered_map<int, Request::BGet *> &bgm_list) {
    return do_operation<Response::BGet>(bgm_list, engine, rpc, endpoints, packed, responses, arrays, buffers);
}

/**
 * BGet2
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGET messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BGet2 *EndpointGroup::communicate(const std::unordered_map<int, Request::BGet2 *> &bgm_list) {
    return do_operation<Response::BGet2>(bgm_list, engine, rpc, endpoints, packed, responses, arrays, buffers);
}

/**
 * BGetOp
 *
 * @param num_rangesrvs the total number of range servers
 * @param bgm_list the list of BGETOP messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BGetOp *EndpointGroup::communicate(const std::unordered_map<int, Request::BGetOp *> &bgm_list) {
    return do_operation<Response::BGetOp>(bgm_list, engine, rpc, endpoints, packed, responses, arrays, buffers);
}

/**
 * BDelete
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BDelete *EndpointGroup::communicate(const std::unordered_map<int, Request::BDelete *> &bdm_list) {
    return do_operation<Response::BDelete>(bdm_list, engine, rpc, endpoints, packed, responses, arrays, buffers);
}

/**
 * BHistogram
 *
 * @param num_rangesrvs the total number of range servers
 * @param bdm_list the list of BDELETE messages to send
 * @return a linked list of response messages, or nullptr
 */
Response::BHistogram *EndpointGroup::communicate(const std::unordered_map<int, Request::BHistogram *> &bhist_list) {
    return do_operation<Response::BHistogram>(bhist_list, engine, rpc, endpoints, packed, responses, arrays, buffers);
}

}
}

#endif
