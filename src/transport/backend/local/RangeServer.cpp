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

Message::Response::Response *range_server(hxhim_t *hx, Message::Request::Request *req) {
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_SERVER_INFO, "Rank %d Local RangeServer got a %s", rank, HXHIM_OP_STR[req->op]);

    Message::Response::Response *res = nullptr;

    // Call the appropriate function depending on the message type
    switch(req->op) {
        case hxhim_op_t::HXHIM_PUT:
            res = range_server<Message::Response::BPut>(hx, static_cast<Message::Request::BPut *>(req));
            break;
        case hxhim_op_t::HXHIM_GET:
            res = range_server<Message::Response::BGet>(hx, static_cast<Message::Request::BGet *>(req));
            break;
        case hxhim_op_t::HXHIM_GETOP:
            res = range_server<Message::Response::BGetOp>(hx, static_cast<Message::Request::BGetOp *>(req));
            break;
        case hxhim_op_t::HXHIM_DELETE:
            res = range_server<Message::Response::BDelete>(hx, static_cast<Message::Request::BDelete *>(req));
            break;
        case hxhim_op_t::HXHIM_HISTOGRAM:
            res = range_server<Message::Response::BHistogram>(hx, static_cast<Message::Request::BHistogram *>(req));
            break;
        default:
            break;
    }

    mlog(HXHIM_SERVER_INFO, "Rank %d Local RangeServer done processing request", rank);
    return res;
}

}
}
