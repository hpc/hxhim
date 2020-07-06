#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "hxhim/accessors.hpp"
#include "hxhim/hxhim.hpp"
#include "transport/backend/local/RangeServer.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"

namespace Transport {
namespace local {

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

Response::Response *range_server(hxhim_t *hx, Request::Request *req) {
    int rank = -1;
    hxhim::GetMPI(hx, nullptr, &rank, nullptr);

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
        // case Message::BHISTOGRAM:
        //     res = range_server<Response::BHistogram>(hx, static_cast<Request::BHistogram *>(req));
        //     break;
        default:
            break;
    }

    mlog(HXHIM_SERVER_INFO, "Rank %d Local RangeServer done processing request", rank);
    return res;
}

}
}
