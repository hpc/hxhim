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
 * Send a message to the local range server
 *
 * @param hx    main HXHIM struct
 * @param msg   pointer to the message
 * @return return_message structure with status = HXHIM_SUCCESS or HXHIM_ERROR
 */
template <typename Request_t, typename Response_t,
          typename = enable_if_t <is_child_of<Request::Request,   Request_t> ::value &&
                                  is_child_of<Response::Response, Response_t>::value> >
Response_t *client(hxhim_t *hx, Request_t *req) {
    return range_server<Response_t>(hx, req);
}

}
}

#endif
