#include "hxhim/local_client.hpp"

#include "hxhim/range_server.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * local_transport
 * force a copy of the data in order to maintain the
 * same ownership semantics when processing local data
 */
static Transport::Request::Request *local_transport(const Transport::Request::Request *orig) {
    void *buf = nullptr;
    std::size_t size = 0;

    // pack the request
    if (Transport::Packer::pack(orig, &buf, &size) != TRANSPORT_SUCCESS) {
        dealloc(buf);
        mlog(THALLIUM_WARN, "Unable to pack message");
        return nullptr;
    }

    // unpack the request
    Transport::Request::Request *copy = nullptr;
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
 *
 * @param hx    main HXHIM struct
 * @param msg   pointer to the message
 * @return return_message structure with status = HXHIM_SUCCESS or HXHIM_ERROR
 */
Transport::Response::Response *hxhim::local_client(hxhim_t *hx, const Transport::Request::Request *req) {
    Transport::Request::Request *copy = local_transport(req);
    Transport::Response::Response *res = static_cast<Transport::Response::Response *>(hxhim::range_server::range_server(hx, copy));
    destruct(copy);
    return res;
}
