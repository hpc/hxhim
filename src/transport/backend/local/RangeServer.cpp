#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "hxhim/hxhim.hpp"
#include "hxhim/private/accessors.hpp"
#include "transport/backend/local/RangeServer.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"

namespace Transport {
namespace local {

Response::Response *range_server(hxhim_t *hx, Request::Request *req) {
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_SERVER_INFO, "Rank %d Local RangeServer got a %s", rank, Message::TypeStr[req->type]);

    Response::Response *res = nullptr;

    // Call the appropriate function depending on the message type
    switch(req->type) {
        case Message::BPUT:
            res = range_server<Response::BPut>(hx, static_cast<Request::BPut *>(req));
            break;
        case Message::BGET:
            res = range_server<Response::BGet>(hx, static_cast<Request::BGet *>(req));
            break;
        case Message::BGETOP:
            res = range_server<Response::BGetOp>(hx, static_cast<Request::BGetOp *>(req));
            break;
        case Message::BDELETE:
            res = range_server<Response::BDelete>(hx, static_cast<Request::BDelete *>(req));
            break;
        default:
            break;
    }

    mlog(HXHIM_SERVER_INFO, "Rank %d Local RangeServer done processing request", rank);
    return res;
}

}
}
