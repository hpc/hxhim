#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "hxhim/private.hpp"
#include "hxhim/range_server.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"

namespace hxhim {
namespace range_server {

/**
 * is_range_server
 *
 * @param rank              the rank of the process
 * @param client_ratio      the client portion of the client to server ratio
 * @param server_ratio      the server portion of the client to server ratio
 * @param true or false
 */
bool is_range_server(const int rank, const std::size_t client_ratio, const std::size_t server_ratio) {
    if (!client_ratio ||
        !server_ratio) {
        return false;
    }

    return ((rank % client_ratio) < server_ratio);
}

// /**
//  * histogram
//  * Gets the histograms from the selected datastores
//  *
//  * @param hx        pointer to the main HXHIM struct
//  * @param bhist     the request packet to operate on
//  * @return          the response packet resulting from the request
//  */
// Transport::Response::BHistogram *bhistogram(hxhim_t *hx, Transport::Request::BHistogram *bhist) {
//     Transport::Response::BHistogram *ret = construct<Transport::Response::BHistogram>(bhist->count);
//     ret->src = bhist->dst;
//     ret->dst = bhist->src;

//     for(std::size_t i = 0; i < bhist->count; i++) {
//         ret->ds_offsets[i] = bhist->ds_offsets[i];

//         Transport::Response::Histogram *res = hx->p->datastore.datastores[bhist->ds_offsets[i]]->Histogram();
//         if (res) {
//             ret->statuses[i] = res->status;
//             ret->hists[i].buckets = res->hist.buckets;
//             ret->hists[i].counts = res->hist.counts;
//             ret->hists[i].size = res->hist.size;
//             destruct(res);
//         }
//         else {
//             ret->statuses[i] = HXHIM_ERROR;
//         }

//         ret->count++;
//     }

//     return ret;
// }

Transport::Response::Response *range_server(hxhim_t *hx, Transport::Request::Request *req) {
    mlog(HXHIM_SERVER_INFO, "Range server started");
    using namespace Transport;

    Response::Response *res = nullptr;
    mlog(HXHIM_SERVER_INFO, "Range server got a %s", Message::TypeStr[req->type]);

    // Call the appropriate function depending on the message type
    switch(req->type) {
        case Message::BPUT:
            res = range_server<Response::BPut>(hx, static_cast<Request::BPut *>(req));
            break;
        case Message::BGET:
            res = range_server<Response::BGet>(hx, static_cast<Request::BGet *>(req));
            break;
        case Message::BGET2:
            res = range_server<Response::BGet2>(hx, static_cast<Request::BGet2 *>(req));
            break;
        case Message::BGETOP:
            res = range_server<Response::BGetOp>(hx, static_cast<Request::BGetOp *>(req));
            break;
        case Message::BDELETE:
            res = range_server<Response::BDelete>(hx, static_cast<Request::BDelete *>(req));
            break;
        // case Message::BHISTOGRAM:
        //     res = range_server<Response::BHistogram>(hx, static_cast<Request::BHistogram *>(req));
        //     break;
        default:
            break;
    }

    mlog(HXHIM_SERVER_INFO, "Range server stopping");
    return res;
}

}
}
