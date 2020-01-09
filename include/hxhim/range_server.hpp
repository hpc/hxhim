#ifndef HXHIM_RANGE_SERVER_HPP
#define HXHIM_RANGE_SERVER_HPP

#include "hxhim/private.hpp"
#include "transport/Messages/Messages.hpp"
#include "utils/type_traits.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace hxhim {
namespace range_server {

/** @description Utility function to determine whether or not a rank has a range server */
bool is_range_server(const int rank, const std::size_t client_ratio, const std::size_t server_ratio);

Transport::Response::Response *range_server(hxhim_t *hx, Transport::Request::Request *req);

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
template <typename Response_t, typename Request_t, typename = enable_if_t <is_child_of<Transport::Request::Request,   Request_t> ::value &&
                                                                           is_child_of<Transport::Response::Response, Response_t>::value> >
Response_t *range_server(hxhim_t *hx, Request_t *req) {
    mlog(HXHIM_SERVER_INFO, "Range server %s %zu", Transport::Message::TypeStr[req->type], req->count);

    // overallocate in case all of the requests go to one datastore
    Request_t *dsts = alloc_array<Request_t>(hx->p->datastore.count, req->count);

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

    // send to each datastore
    for(std::size_t ds = 0; ds < hx->p->datastore.count; ds++) {
        Response_t *response = hx->p->datastore.datastores[ds]->operate(&dsts[ds]);

        // if there were responses, copy them into the output variable
        if (response) {
            for(std::size_t i = 0; i < response->count; i++) {
                res->steal(response, i);
            }
            response->count = 0;
            destruct(response);
        }
    }

    dealloc_array(dsts, hx->p->datastore.count);

    mlog(HXHIM_SERVER_INFO, "Range server %s completed", Transport::Message::TypeStr[req->type]);
    return res;
}


}
}

#endif
