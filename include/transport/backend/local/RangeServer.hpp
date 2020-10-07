#ifndef HXHIM_RANGE_SERVER_HPP
#define HXHIM_RANGE_SERVER_HPP

#include "datastore/datastore.hpp"
#include "hxhim/constants.h"
#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"
#include "transport/Messages/Messages.hpp"
#include "utils/Stats.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"
#include "utils/type_traits.hpp"

namespace Transport {
namespace local {

Response::Response *range_server(hxhim_t *hx, Request::Request *req);

/**
 * bput
 * Handles the bput message and puts data in the datastore
 *
 * @tparam Response_t  Transport::Response::*
 * @tparam Request_t   Transport::Request::*
 * @param hx           pointer to the main HXHIM struct
 * @param req          the request packet to operate on
 * @return             the response packet resulting from the request
 */
template <typename Response_t, typename Request_t,
          typename = enable_if_t <is_child_of<Request::Request,   Request_t> ::value &&
                                  is_child_of<Response::Response, Response_t>::value> >
Response_t *range_server(hxhim_t *hx, Request_t *req) {
    req->timestamps.transport->start = ::Stats::now();

    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_SERVER_INFO, "Rank %d Local RangeServer recevied %s request", rank, HXHIM_OP_STR[req->op]);

    // final response variable
    Response_t *res = construct<Response_t>(req->count);
    res->src = req->dst;
    res->dst = req->src;
    res->steal_timestamps(req, false);

    // no packing
    res->timestamps.transport->pack.start = ::Stats::now();
    res->timestamps.transport->pack.end = res->timestamps.transport->pack.start;

    // send to each datastore
    res->timestamps.transport->send_start = ::Stats::now();

    Response_t *response = hx->p->datastore->operate(req);

    // if there were responses, copy them into the output variable
    if (response) {
        for(std::size_t i = 0; i < response->count; i++) {
            // move timestamps over to response (responses should correspond to requests)
            response->timestamps.reqs[i] = req->timestamps.reqs[i];
            req->timestamps.reqs[i] = nullptr;

            // move the datastore response to the main response
            res->steal(response, i);
        }

        response->count = 0;
        destruct(response);
    }

    res->timestamps.transport->recv_end = ::Stats::now();

    // no unpacking
    res->timestamps.transport->unpack.start = ::Stats::now();
    res->timestamps.transport->unpack.end = res->timestamps.transport->unpack.start;

    // no clean up rpc
    res->timestamps.transport->cleanup_rpc.start = ::Stats::now();
    res->timestamps.transport->cleanup_rpc.end = res->timestamps.transport->cleanup_rpc.start;

    mlog(HXHIM_SERVER_INFO, "Rank %d Local RangeServer done processing %s", rank, HXHIM_OP_STR[req->op]);
    res->timestamps.transport->end = ::Stats::now();

    return res;
}

}
}

#endif
