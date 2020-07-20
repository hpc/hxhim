#ifndef HXHIM_RANGE_SERVER_HPP
#define HXHIM_RANGE_SERVER_HPP

#include <vector>

#include "datastore/datastore.hpp"
#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"
#include "transport/Messages/Messages.hpp"
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
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_SERVER_INFO, "Rank %d Local RangeServer recevied %s request", rank, Message::TypeStr[req->type]);

    // overallocate in case all of the requests go to one datastore
    std::size_t datastore_count = 0;
    hxhim::GetDatastoresPerRangeServer(hx, &datastore_count);
    Request_t *dsts = alloc_array<Request_t>(datastore_count, req->count);

    // split up requests into datastores
    // pointers are now owned by the database inputs, not by the server request
    for(std::size_t i = 0; i < req->count; i++) {
        const int datastore = req->ds_offsets[i];
        Request_t &dst = dsts[datastore];
        dst.steal(req, i);
    }

    // response variable
    Response_t *res = construct<Response_t>(req->count);
    res->src = req->dst;
    res->dst = req->src;
    for(std::size_t i = 0; i < req->count; i++) {
        res->timestamps.reqs[i] = req->timestamps.reqs[i];
    }
    res->timestamps.transport = req->timestamps.transport;

    std::vector<hxhim::datastore::Datastore *> *datastores = &hx->p->datastores;

    // send to each datastore
    for(std::size_t ds = 0; ds < datastore_count; ds++) {
        // no packing
        res->timestamps.transport.pack.start = hx->p->epoch;
        res->timestamps.transport.pack.end = hx->p->epoch;

        clock_gettime(CLOCK_MONOTONIC, &res->timestamps.transport.send_start);
        Response_t *response = (*datastores)[ds]->operate(&dsts[ds]);
        clock_gettime(CLOCK_MONOTONIC, &res->timestamps.transport.recv_end);

        // no unpacking
        res->timestamps.transport.unpack.start = hx->p->epoch;
        res->timestamps.transport.unpack.end = hx->p->epoch;

        // if there were responses, copy them into the output variable
        if (response) {
            for(std::size_t i = 0; i < response->count; i++) {
                res->steal(response, i);
            }

            response->count = 0;
            destruct(response);
        }
    }

    dealloc_array(dsts, datastore_count);

    mlog(HXHIM_SERVER_INFO, "Rank %d Local RangeServer done processing %s", rank, Message::TypeStr[req->type]);
    return res;
}

}
}

#endif
