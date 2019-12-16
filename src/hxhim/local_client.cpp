#include "hxhim/local_client.hpp"

#include "hxhim/range_server.hpp"
#include "hxhim/struct.h"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * local_transport
 * force a copy of the data in order to maintain the
 * same ownership semantics when processing local data
 */
template <typename Msg_t>
static Msg_t *local_transport(Msg_t *src) {
    void *buf = nullptr;
    std::size_t size = 0;

    // pack the message
    if (Transport::Packer::pack(src, &buf, &size) != TRANSPORT_SUCCESS) {
        dealloc(buf);
        mlog(THALLIUM_WARN, "Unable to pack message");
        return nullptr;
    }

    // unpack the message
    Msg_t *copy = nullptr;
    if (Transport::Unpacker::unpack(&copy, buf, size) != TRANSPORT_SUCCESS) {
        dealloc(buf);
        mlog(THALLIUM_DBG, "Could not unpack message");
        return nullptr;
    }

    dealloc(buf);
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
Transport::Response::Response *hxhim::local_client(hxhim_t *hx, Transport::Request::Request *req) {
    // send to "server"
    Transport::Request::Request *remote_req = local_transport(req);
    // do not destruct req here since it was not created here

    // process
    Transport::Response::Response *remote_res = hxhim::range_server::range_server(hx, remote_req);
    destruct(remote_req);

    // send to "client"
    Transport::Response::Response *res = local_transport(remote_res);
    destruct(remote_res);

    return res;
}
