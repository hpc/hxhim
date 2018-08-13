#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "hxhim/options.h"
#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/range_server.hpp"

/**
 * range_server_bput
 * Handles the bput message and puts data in the datastore
 *
 * @param hx        pointer to the main HXHIM struct
 * @param req       the request packet to operate on
 * @return          the response packet resulting from the request
 */
static Transport::Response::BPut *range_server_bput(hxhim_t *hx, Transport::Request::BPut *req) {
    void ***subjects = new void **[hx->p->datastore_count];
    std::size_t **subject_lens = new std::size_t *[hx->p->datastore_count];
    void ***predicates = new void **[hx->p->datastore_count];
    std::size_t **predicate_lens = new std::size_t *[hx->p->datastore_count];
    hxhim_type_t **object_types = new hxhim_type_t *[hx->p->datastore_count];
    void ***objects = new void **[hx->p->datastore_count];
    std::size_t **object_lens = new std::size_t *[hx->p->datastore_count];
    std::size_t *counters = new std::size_t[hx->p->datastore_count];

    // overallocate in case all of the requests go to one datastore
    for(std::size_t i = 0; i < hx->p->datastore_count; i++) {
        subjects[i] = new void *[req->count];
        subject_lens[i] = new std::size_t[req->count];
        predicates[i] = new void *[req->count];
        predicate_lens[i] = new std::size_t[req->count];
        object_types[i] = new hxhim_type_t[req->count];
        objects[i] = new void *[req->count];
        object_lens[i] = new std::size_t[req->count];
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

    Transport::Response::BPut *res = new Transport::Response::BPut(req->count);
    res->src = req->dst;
    res->dst = req->src;
    res->count = 0; // reset counter, but keep allocated space

    for(std::size_t i = 0; i < hx->p->datastore_count; i++) {
        Transport::Response::BPut *response = hx->p->datastores[i]->BPut(subjects[i], subject_lens[i],
                                                                        predicates[i], predicate_lens[i],
                                                                        object_types[i], objects[i], object_lens[i],
                                                                        counters[i]);
        for(std::size_t j = 0; j < response->count; j++) {
            res->ds_offsets[res->count] = i;
            res->statuses[res->count] = response->statuses[j];
            res->count++;
        }

        delete response;
    }

    for(std::size_t i = 0; i < hx->p->datastore_count; i++) {
        delete [] subjects[i];
        delete [] subject_lens[i];
        delete [] predicates[i];
        delete [] predicate_lens[i];
        delete [] object_types[i];
        delete [] objects[i];
        delete [] object_lens[i];
    }

    delete [] subjects;
    delete [] subject_lens;
    delete [] predicates;
    delete [] predicate_lens;
    delete [] object_types;
    delete [] objects;
    delete [] object_lens;
    delete [] counters;

    return res;
}

/**
 * range_server_bget
 * Handles the bget message and gets data in the datastore
 *
 * @param hx        pointer to the main HXHIM struct
 * @param req       the request packet to operate on
 * @return          the response packet resulting from the request
 */
static Transport::Response::BGet *range_server_bget(hxhim_t *hx, Transport::Request::BGet *req) {
    void ***subjects = new void **[hx->p->datastore_count];
    std::size_t **subject_lens = new std::size_t *[hx->p->datastore_count];
    void ***predicates = new void **[hx->p->datastore_count];
    std::size_t **predicate_lens = new std::size_t *[hx->p->datastore_count];
    hxhim_type_t **object_types = new hxhim_type_t *[hx->p->datastore_count];
    std::size_t *counters = new std::size_t[hx->p->datastore_count];

    // overallocate in case all of the requests go to one datastore
    for(std::size_t i = 0; i < hx->p->datastore_count; i++) {
        subjects[i] = new void *[req->count];
        subject_lens[i] = new std::size_t[req->count];
        predicates[i] = new void *[req->count];
        predicate_lens[i] = new std::size_t[req->count];
        object_types[i] = new hxhim_type_t[req->count];
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

    Transport::Response::BGet *res = new Transport::Response::BGet(req->count);
    res->src = req->dst;
    res->dst = req->src;

    for(std::size_t i = 0; i < hx->p->datastore_count; i++) {
        Transport::Response::BGet *response = hx->p->datastores[i]->BGet(subjects[i], subject_lens[i],
                                                                       predicates[i], predicate_lens[i],
                                                                       object_types[i],
                                                                       counters[i]);
        for(std::size_t j = 0; j < response->count; j++) {
            res->ds_offsets[res->count] = i;
            res->statuses[res->count] = response->statuses[j];
            res->subjects[res->count] = std::move(response->subjects[j]);
            res->subject_lens[res->count] = response->subject_lens[j];
            res->predicates[res->count] = std::move(response->predicates[j]);
            res->predicate_lens[res->count] = response->predicate_lens[j];
            res->object_types[res->count] = std::move(response->object_types[j]);
            res->objects[res->count] = std::move(response->objects[j]);
            res->object_lens[res->count] = response->object_lens[j];
            res->count++;
        }

        delete response;
    }

    for(std::size_t i = 0; i < hx->p->datastore_count; i++) {
        delete [] subjects[i];
        delete [] subject_lens[i];
        delete [] predicates[i];
        delete [] predicate_lens[i];
        delete [] object_types[i];
    }

    delete [] subjects;
    delete [] subject_lens;
    delete [] predicates;
    delete [] predicate_lens;
    delete [] object_types;
    delete [] counters;

    return res;
}

/**
 * range_server_bgetop
 * Handles the bgetop message and getops data in the datastore
 *
 * @param hx        pointer to the main HXHIM struct
 * @param req       the request packet to operate on
 * @return          the response packet resulting from the request
 */
static Transport::Response::BGetOp *range_server_bgetop(hxhim_t *hx, Transport::Request::BGetOp *req) {
    Transport::Response::BGetOp *res = new Transport::Response::BGetOp(req->count);
    res->src = req->dst;
    res->dst = req->src;

    for(std::size_t i = 0; i < req->count; i++) {
        Transport::Response::BGetOp *response = hx->p->datastores[i]->BGetOp(req->subjects[i], req->subject_lens[i],
                                                                           req->predicates[i], req->predicate_lens[i],
                                                                           req->object_types[i],
                                                                           req->num_recs[i], req->ops[i]);
        for(std::size_t j = 0; j < response->count; j++) {
            res->ds_offsets[res->count] = i;
            res->statuses[res->count] = response->statuses[j];
            res->subjects[res->count] = std::move(response->subjects[j]);
            res->subject_lens[res->count] = response->subject_lens[j];
            res->predicates[res->count] = std::move(response->predicates[j]);
            res->predicate_lens[res->count] = response->predicate_lens[j];
            res->object_types[res->count] = std::move(response->object_types[j]);
            res->objects[res->count] = std::move(response->objects[j]);
            res->object_lens[res->count] = response->object_lens[j];
            res->count++;
        }

        delete response;
    }

    return res;
}

/**
 * range_server_bdelete
 * Handles the bdelete message and deletes data in the datastore
 *
 * @param hx        pointer to the main HXHIM struct
 * @param req       the request packet to operate on
 * @return          the response packet resulting from the request
 */
static Transport::Response::BDelete *range_server_bdelete(hxhim_t *hx, Transport::Request::BDelete *req) {
    void ***subjects = new void **[hx->p->datastore_count];
    std::size_t **subject_lens = new std::size_t *[hx->p->datastore_count];
    void ***predicates = new void **[hx->p->datastore_count];
    std::size_t **predicate_lens = new std::size_t *[hx->p->datastore_count];
    std::size_t *counters = new std::size_t[hx->p->datastore_count];

    // overallocate in case all of the requests go to one datastore
    for(std::size_t i = 0; i < hx->p->datastore_count; i++) {
        subjects[i] = new void *[req->count];
        subject_lens[i] = new std::size_t[req->count];
        predicates[i] = new void *[req->count];
        predicate_lens[i] = new std::size_t[req->count];
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

    Transport::Response::BDelete *res = new Transport::Response::BDelete(req->count);
    res->src = req->dst;
    res->dst = req->src;

    for(std::size_t i = 0; i < hx->p->datastore_count; i++) {
        Transport::Response::BDelete *response = hx->p->datastores[i]->BDelete(subjects[i], subject_lens[i],
                                                                             predicates[i], predicate_lens[i],
                                                                             counters[i]);
        for(std::size_t j = 0; j < response->count; j++) {
            res->ds_offsets[res->count] = i;
            res->statuses[res->count] = response->statuses[j];
            res->count++;
        }

        delete response;
    }

    for(std::size_t i = 0; i < hx->p->datastore_count; i++) {
        delete [] subjects[i];
        delete [] subject_lens[i];
        delete [] predicates[i];
        delete [] predicate_lens[i];
    }

    delete [] subjects;
    delete [] subject_lens;
    delete [] predicates;
    delete [] predicate_lens;
    delete [] counters;

    return res;
}

Transport::Response::Response *range_server(hxhim_t *hx, Transport::Request::Request *req) {
    Transport::Response::Response *res = nullptr;

    // Call the appropriate function depending on the message type
    switch(req->type) {
        case Transport::Message::BPUT:
            res = range_server_bput(hx, dynamic_cast<Transport::Request::BPut *>(req));
            break;
        case Transport::Message::BGET:
            res = range_server_bget(hx, dynamic_cast<Transport::Request::BGet *>(req));
            break;
        case Transport::Message::BGETOP:
            res = range_server_bgetop(hx, dynamic_cast<Transport::Request::BGetOp *>(req));
            break;
        case Transport::Message::BDELETE:
            res = range_server_bdelete(hx, dynamic_cast<Transport::Request::BDelete *>(req));
            break;
            // case Transport::Message::SYNC:
            //     break;
            // case Transport::Message::HISTOGRAM:
            //     break;
        default:
            break;
    }

    return res;
}
