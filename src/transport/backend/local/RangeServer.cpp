#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "hxhim/Blob.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/private/accessors.hpp"
#include "transport/backend/local/RangeServer.hpp"
#include "utils/memory.hpp"

namespace Transport {
namespace local {

Response::Response *range_server(hxhim_t *hx, Request::Request *req) {
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_SERVER_INFO, "Rank %d Local RangeServer got a %s", rank, HXHIM_OP_STR[req->op]);

    Response::Response *res = nullptr;

    // Call the appropriate function depending on the message type
    switch(req->op) {
        case hxhim_op_t::HXHIM_PUT:
            res = range_server<Response::BPut>(hx, static_cast<Request::BPut *>(req));
            break;
        case hxhim_op_t::HXHIM_GET:
            res = range_server<Response::BGet>(hx, static_cast<Request::BGet *>(req));
            break;
        case hxhim_op_t::HXHIM_GETOP:
            res = range_server<Response::BGetOp>(hx, static_cast<Request::BGetOp *>(req));
            break;
        case hxhim_op_t::HXHIM_DELETE:
            res = range_server<Response::BDelete>(hx, static_cast<Request::BDelete *>(req));
            break;
        case hxhim_op_t::HXHIM_HISTOGRAM:
            res = range_server<Response::BHistogram>(hx, static_cast<Request::BHistogram *>(req));
            break;
        default:
            break;
    }

    mlog(HXHIM_SERVER_INFO, "Rank %d Local RangeServer done processing request", rank);
    return res;
}

}
}
