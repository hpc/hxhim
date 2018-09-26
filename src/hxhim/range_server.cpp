#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "hxhim/options.h"
#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/range_server.hpp"

namespace hxhim {
namespace range_server {

/**
 * bput
 * Handles the bput message and puts data in the datastore
 *
 * @param hx        pointer to the main HXHIM struct
 * @param req       the request packet to operate on
 * @return          the response packet resulting from the request
 */
static Transport::Response::BPut *bput(hxhim_t *hx, const Transport::Request::BPut *req) {
    FixedBufferPool *responses = hx->p->memory_pools.responses;
    FixedBufferPool *arrays = hx->p->memory_pools.arrays;
    FixedBufferPool *buffers = hx->p->memory_pools.buffers;

    void ***subjects = arrays->acquire_array<void **>(hx->p->datastore.count);
    std::size_t **subject_lens = arrays->acquire_array<std::size_t *>(hx->p->datastore.count);
    void ***predicates = arrays->acquire_array<void **>(hx->p->datastore.count);
    std::size_t **predicate_lens = arrays->acquire_array<std::size_t *>(hx->p->datastore.count);
    hxhim_type_t **object_types = arrays->acquire_array<hxhim_type_t *>(hx->p->datastore.count);
    void ***objects = arrays->acquire_array<void **>(hx->p->datastore.count);
    std::size_t **object_lens = arrays->acquire_array<std::size_t *>(hx->p->datastore.count);
    std::size_t *counters = arrays->acquire_array<std::size_t>(hx->p->datastore.count);

    // overallocate in case all of the requests go to one datastore
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        subjects[i] = arrays->acquire_array<void *>(req->count);
        subject_lens[i] = arrays->acquire_array<std::size_t>(req->count);
        predicates[i] = arrays->acquire_array<void *>(req->count);
        predicate_lens[i] = arrays->acquire_array<std::size_t>(req->count);
        object_types[i] = arrays->acquire_array<hxhim_type_t>(req->count);
        objects[i] = arrays->acquire_array<void *>(req->count);
        object_lens[i] = arrays->acquire_array<std::size_t>(req->count);
        counters[i] = 0;
    }

    // split up requests into datastores
    for(std::size_t i = 0; i < req->count; i++) {
        const int datastore = req->ds_offsets[i];
        std::size_t &index = counters[req->ds_offsets[i]];

        subjects[datastore][index] = req->subjects[i];
        subject_lens[datastore][index] = req->subject_lens[i];

        predicates[datastore][index] = req->predicates[i];
        predicate_lens[datastore][index] = req->predicate_lens[i];

        object_types[datastore][index] = req->object_types[i];
        objects[datastore][index] = req->objects[i];
        object_lens[datastore][index] = req->object_lens[i];

        index++;
    }

    // set up output variable
    Transport::Response::BPut *res = responses->acquire<Transport::Response::BPut>(arrays, buffers, req->count);
    res->src = req->dst;
    res->dst = req->src;

    // BPUT to each datastore
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        Transport::Response::BPut *response = hx->p->datastore.datastores[i]->BPut(subjects[i], subject_lens[i],
                                                                                   predicates[i], predicate_lens[i],
                                                                                   object_types[i], objects[i], object_lens[i],
                                                                                   counters[i]);
        // if there were responses, copy them into the output variable
        if (response) {
            for(std::size_t j = 0; j < response->count; j++) {
                res->ds_offsets[res->count] = i;
                res->statuses[res->count] = response->statuses[j];
                res->count++;
            }

            responses->release(response);
        }
    }

    // clean up each datastore's input array
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        arrays->release_array(subjects[i], counters[i]);
        arrays->release_array(subject_lens[i], counters[i]);
        arrays->release_array(predicates[i], counters[i]);
        arrays->release_array(predicate_lens[i], counters[i]);
        arrays->release_array(object_types[i], counters[i]);
        arrays->release_array(objects[i], counters[i]);
        arrays->release_array(object_lens[i], counters[i]);
    }
    arrays->release_array(subjects, hx->p->datastore.count);
    arrays->release_array(subject_lens, hx->p->datastore.count);
    arrays->release_array(predicates, hx->p->datastore.count);
    arrays->release_array(predicate_lens, hx->p->datastore.count);
    arrays->release_array(object_types, hx->p->datastore.count);
    arrays->release_array(objects, hx->p->datastore.count);
    arrays->release_array(object_lens, hx->p->datastore.count);
    arrays->release_array(counters, hx->p->datastore.count);

    return res;
}

/**
 * bget
 * Handles the bget message and gets data in the datastore
 *
 * @param hx        pointer to the main HXHIM struct
 * @param req       the request packet to operate on
 * @return          the response packet resulting from the request
 */
static Transport::Response::BGet *bget(hxhim_t *hx, const Transport::Request::BGet *req) {
    FixedBufferPool *responses = hx->p->memory_pools.responses;
    FixedBufferPool *arrays = hx->p->memory_pools.arrays;
    FixedBufferPool *buffers = hx->p->memory_pools.buffers;

    void ***subjects = arrays->acquire_array<void **>(hx->p->datastore.count);
    std::size_t **subject_lens = arrays->acquire_array<std::size_t *>(hx->p->datastore.count);
    void ***predicates = arrays->acquire_array<void **>(hx->p->datastore.count);
    std::size_t **predicate_lens = arrays->acquire_array<std::size_t *>(hx->p->datastore.count);
    hxhim_type_t **object_types = arrays->acquire_array<hxhim_type_t *>(hx->p->datastore.count);
    std::size_t *counters = arrays->acquire_array<std::size_t>(hx->p->datastore.count);

    // overallocate in case all of the requests go to one datastore
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        subjects[i] = arrays->acquire_array<void *>(req->count);
        subject_lens[i] = arrays->acquire_array<std::size_t>(req->count);
        predicates[i] = arrays->acquire_array<void *>(req->count);
        predicate_lens[i] = arrays->acquire_array<std::size_t>(req->count);
        object_types[i] = arrays->acquire_array<hxhim_type_t>(req->count);
        counters[i] = 0;
    }

    // split up requests into specific datastores
    for(std::size_t i = 0; i < req->count; i++) {
        const int datastore = req->ds_offsets[i];
        std::size_t &index = counters[req->ds_offsets[i]];

        subjects[datastore][index] = req->subjects[i];
        subject_lens[datastore][index] = req->subject_lens[i];

        predicates[datastore][index] = req->predicates[i];
        predicate_lens[datastore][index] = req->predicate_lens[i];

        object_types[datastore][index] = req->object_types[i];

        index++;
    }

    Transport::Response::BGet *res = responses->acquire<Transport::Response::BGet>(arrays, buffers, req->count);
    res->src = req->dst;
    res->dst = req->src;

    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        Transport::Response::BGet *response = hx->p->datastore.datastores[i]->BGet(subjects[i], subject_lens[i],
                                                                                   predicates[i], predicate_lens[i],
                                                                                   object_types[i],
                                                                                   counters[i]);
        if (response) {
            for(std::size_t j = 0; j < response->count; j++) {
                res->ds_offsets[res->count] = i;
                res->statuses[res->count] = response->statuses[j];
                std::swap(res->subjects[res->count], response->subjects[j]);
                res->subject_lens[res->count] = response->subject_lens[j];
                std::swap(res->predicates[res->count], response->predicates[j]);
                res->predicate_lens[res->count] = response->predicate_lens[j];
                std::swap(res->object_types[res->count], response->object_types[j]);
                if (res->statuses[res->count] == HXHIM_SUCCESS) {
                    std::swap(res->objects[res->count], response->objects[j]);
                    res->object_lens[res->count] = response->object_lens[j];
                }
                res->count++;
            }

            responses->release(response);
        }
    }

    // clean up each datastore's input array
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        arrays->release_array(subjects[i], counters[i]);
        arrays->release_array(subject_lens[i], counters[i]);
        arrays->release_array(predicates[i], counters[i]);
        arrays->release_array(predicate_lens[i], counters[i]);
        arrays->release_array(object_types[i], counters[i]);
    }
    arrays->release_array(subjects, hx->p->datastore.count);
    arrays->release_array(subject_lens, hx->p->datastore.count);
    arrays->release_array(predicates, hx->p->datastore.count);
    arrays->release_array(predicate_lens, hx->p->datastore.count);
    arrays->release_array(object_types, hx->p->datastore.count);
    arrays->release_array(counters, hx->p->datastore.count);

    return res;
}

/**
 * bgetop
 * Handles the bgetop message and getops data in the datastore
 *
 * @param hx        pointer to the main HXHIM struct
 * @param req       the request packet to operate on
 * @return          the response packet resulting from the request
 */
static Transport::Response::BGetOp *bgetop(hxhim_t *hx, const Transport::Request::BGetOp *req) {
    FixedBufferPool *responses = hx->p->memory_pools.responses;
    FixedBufferPool *arrays = hx->p->memory_pools.arrays;
    FixedBufferPool *buffers = hx->p->memory_pools.buffers;

    Transport::Response::BGetOp *res = responses->acquire<Transport::Response::BGetOp>(arrays, buffers, req->count);
    res->src = req->dst;
    res->dst = req->src;

    for(std::size_t i = 0; i < req->count; i++) {
        Transport::Response::BGetOp *response = hx->p->datastore.datastores[i]->BGetOp(req->subjects[i], req->subject_lens[i],
                                                                                       req->predicates[i], req->predicate_lens[i],
                                                                                       req->object_types[i],
                                                                                       req->num_recs[i], req->ops[i]);
        if (response) {
            for(std::size_t j = 0; j < response->count; j++) {
                res->ds_offsets[res->count] = i;
                res->statuses[res->count] = response->statuses[j];
                std::swap(res->subjects[res->count], response->subjects[j]);
                res->subject_lens[res->count] = response->subject_lens[j];
                std::swap(res->predicates[res->count], response->predicates[j]);
                res->predicate_lens[res->count] = response->predicate_lens[j];
                std::swap(res->object_types[res->count], response->object_types[j]);
                if (res->statuses[res->count] == HXHIM_SUCCESS) {
                    std::swap(res->objects[res->count], response->objects[j]);
                    res->object_lens[res->count] = response->object_lens[j];
                }
                res->count++;
            }

            responses->release(response);
        }
    }

    return res;
}

/**
 * bdelete
 * Handles the bdelete message and deletes data in the datastore
 *
 * @param hx        pointer to the main HXHIM struct
 * @param req       the request packet to operate on
 * @return          the response packet resulting from the request
 */
static Transport::Response::BDelete *bdelete(hxhim_t *hx, const Transport::Request::BDelete *req) {
    FixedBufferPool *responses = hx->p->memory_pools.responses;
    FixedBufferPool *arrays = hx->p->memory_pools.arrays;
    FixedBufferPool *buffers = hx->p->memory_pools.buffers;

    void ***subjects = arrays->acquire_array<void **>(hx->p->datastore.count);
    std::size_t **subject_lens = arrays->acquire_array<std::size_t *>(hx->p->datastore.count);
    void ***predicates = arrays->acquire_array<void **>(hx->p->datastore.count);
    std::size_t **predicate_lens = arrays->acquire_array<std::size_t *>(hx->p->datastore.count);
    std::size_t *counters = arrays->acquire_array<std::size_t>(hx->p->datastore.count);

    // overallocate in case all of the requests go to one datastore
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        subjects[i] = arrays->acquire_array<void *>(req->count);
        subject_lens[i] = arrays->acquire_array<std::size_t>(req->count);
        predicates[i] = arrays->acquire_array<void *>(req->count);
        predicate_lens[i] = arrays->acquire_array<std::size_t>(req->count);
        counters[i] = 0;
    }

    // split up requests into specific datastores
    for(std::size_t i = 0; i < req->count; i++) {
        const int datastore = req->ds_offsets[i];
        std::size_t &index = counters[req->ds_offsets[i]];

        subjects[datastore][index] = req->subjects[i];
        subject_lens[datastore][index] = req->subject_lens[i];

        predicates[datastore][index] = req->predicates[i];
        predicate_lens[datastore][index] = req->predicate_lens[i];

        index++;
    }

    Transport::Response::BDelete *res = responses->acquire<Transport::Response::BDelete>(arrays, buffers, req->count);
    res->src = req->dst;
    res->dst = req->src;

    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        Transport::Response::BDelete *response = hx->p->datastore.datastores[i]->BDelete(subjects[i], subject_lens[i],
                                                                                         predicates[i], predicate_lens[i],
                                                                                         counters[i]);
        if (response) {
            for(std::size_t j = 0; j < response->count; j++) {
                res->ds_offsets[res->count] = i;
                res->statuses[res->count] = response->statuses[j];
                res->count++;
            }

            responses->release(response);
        }
    }

    // clean up each datastore's input array
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        arrays->release_array(subjects[i], counters[i]);
        arrays->release_array(subject_lens[i], counters[i]);
        arrays->release_array(predicates[i], counters[i]);
        arrays->release_array(predicate_lens[i], counters[i]);
    }
    arrays->release_array(subjects, hx->p->datastore.count);
    arrays->release_array(subject_lens, hx->p->datastore.count);
    arrays->release_array(predicates, hx->p->datastore.count);
    arrays->release_array(predicate_lens, hx->p->datastore.count);
    arrays->release_array(counters, hx->p->datastore.count);

    return res;
}

/**
 * histogram
 * Gets the histogram from the selected datastore
 *
 * @param hx        pointer to the main HXHIM struct
 * @param hist      the request packet to operate on
 * @return          the response packet resulting from the request
 */
Transport::Response::Histogram *histogram(hxhim_t *hx, const Transport::Request::Histogram *hist) {
    return hx->p->datastore.datastores[hist->ds_offset]->Histogram();
}

/**
 * histogram
 * Gets the histograms from the selected datastores
 *
 * @param hx        pointer to the main HXHIM struct
 * @param bhist     the request packet to operate on
 * @return          the response packet resulting from the request
 */
Transport::Response::BHistogram *bhistogram(hxhim_t *hx, const Transport::Request::BHistogram *bhist) {
    FixedBufferPool *responses = hx->p->memory_pools.responses;
    FixedBufferPool *arrays = hx->p->memory_pools.arrays;
    FixedBufferPool *buffers = hx->p->memory_pools.buffers;

    Transport::Response::BHistogram *ret = responses->acquire<Transport::Response::BHistogram>(arrays, buffers, bhist->count);
    ret->src = bhist->dst;
    ret->dst = bhist->src;

    for(std::size_t i = 0; i < bhist->count; i++) {
        ret->ds_offsets[i] = bhist->ds_offsets[i];

        Transport::Response::Histogram *res = hx->p->datastore.datastores[bhist->ds_offsets[i]]->Histogram();
        if (res) {
            ret->statuses[i] = res->status;
            ret->hists[i].buckets = res->hist.buckets;
            ret->hists[i].counts = res->hist.counts;
            ret->hists[i].size = res->hist.size;
            hx->p->memory_pools.responses->release(res);
        }
        else {
            ret->statuses[i] = HXHIM_ERROR;
        }

        ret->count++;
    }

    return ret;
}

Transport::Response::Response *range_server(hxhim_t *hx, const Transport::Request::Request *req) {
    using namespace Transport;

    Response::Response *res = nullptr;

    // Call the appropriate function depending on the message type
    switch(req->type) {
        case Message::BPUT:
            res = bput(hx, dynamic_cast<const Request::BPut *>(req));
            break;
        case Message::BGET:
            res = bget(hx, dynamic_cast<const Request::BGet *>(req));
            break;
        case Message::BGETOP:
            res = bgetop(hx, dynamic_cast<const Request::BGetOp *>(req));
            break;
        case Message::BDELETE:
            res = bdelete(hx, dynamic_cast<const Request::BDelete *>(req));
            break;
        case Message::HISTOGRAM:
            res = histogram(hx, dynamic_cast<const Request::Histogram *>(req));
            break;
        case Message::BHISTOGRAM:
            res = bhistogram(hx, dynamic_cast<const Request::BHistogram *>(req));
            break;
        default:
            break;
    }

    return res;
}

}
}
