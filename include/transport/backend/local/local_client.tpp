#ifndef TRANSPORT_BACKEND_LOCAL_CLIENT_TPP
#define TRANSPORT_BACKEND_LOCAL_CLIENT_TPP

#include "hxhim/private.hpp"
#include "transport/Messages/Messages.hpp"
#include "transport/backend/local/RangeServer.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace local {

/**
 * local_transport
 * force a copy of the data in order to maintain the
 * same ownership semantics when processing local data
 */
template <typename Msg_t>
Msg_t *transport(Msg_t *src) {
    void *buf = nullptr;
    std::size_t size = 0;

    // pack the message
    if (Packer::pack(src, &buf, &size) != TRANSPORT_SUCCESS) {
        dealloc(buf);
        mlog(THALLIUM_WARN, "Unable to pack message");
        return nullptr;
    }

    // unpack the message
    Msg_t *copy = nullptr;
    if (Unpacker::unpack(&copy, buf, size) != TRANSPORT_SUCCESS) {
        dealloc(buf);
        mlog(THALLIUM_DBG, "Could not unpack message");
        return nullptr;
    }

    dealloc(buf);

    // Msg_t *copy = construct<Msg_t>(src->count);
    // for(std::size_t i = 0; i < src->count; i++) {
    //     copy->steal(src, i);
    // }

    // src->count = 0;

    return copy;
}

/**
 * Send a message to the local range server
 * The data is intentionally copied twice to have
 * the same data semantics as a real networked send
 *
 * @param hx    main HXHIM struct
 * @param msg   pointer to the message
 * @return return_message structure with status = HXHIM_SUCCESS or HXHIM_ERROR
 */
template <typename Request_t, typename Response_t,
          typename = enable_if_t <is_child_of<Request::Request,   Request_t> ::value &&
                                  is_child_of<Response::Response, Response_t>::value> >
Response_t *client(hxhim_t *hx, Request_t *req) {
    // send to "server"
    Request_t *remote_req = transport(req);
    // do not destruct req here since it was not created here

    // process
    Response_t *remote_res = range_server<Response_t>(hx, remote_req);
    destruct(remote_req);

    // send to "client"
    Response_t *res = transport(remote_res);
    mlog(HXHIM_SERVER_INFO, "Range server %s %zu", Message::TypeStr[req->type], req->count);
    destruct(remote_res);

    return res;
}

}
}

#endif
