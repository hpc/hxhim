#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "hxhim/options.h"
#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/range_server.hpp"
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

/**
 * bput
 * Handles the bput message and puts data in the datastore
 *
 * @param hx        pointer to the main HXHIM struct
 * @param req       the request packet to operate on
 * @return          the response packet resulting from the request
 */
static Transport::Response::BPut *bput(hxhim_t *hx, const Transport::Request::BPut *req) {
    mlog(HXHIM_SERVER_INFO, "Range server BPUT %zu", req->count);

    void ***subjects = alloc_array<void **>(hx->p->datastore.count);
    std::size_t **subject_lens = alloc_array<std::size_t *>(hx->p->datastore.count);
    void ***predicates = alloc_array<void **>(hx->p->datastore.count);
    std::size_t **predicate_lens = alloc_array<std::size_t *>(hx->p->datastore.count);
    hxhim_type_t **object_types = alloc_array<hxhim_type_t *>(hx->p->datastore.count);
    void ***objects = alloc_array<void **>(hx->p->datastore.count);
    std::size_t **object_lens = alloc_array<std::size_t *>(hx->p->datastore.count);
    std::size_t *counters = alloc_array<std::size_t>(hx->p->datastore.count);

    // overallocate in case all of the requests go to one datastore
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        subjects[i] = alloc_array<void *>(req->count);
        subject_lens[i] = alloc_array<std::size_t>(req->count);
        predicates[i] = alloc_array<void *>(req->count);
        predicate_lens[i] = alloc_array<std::size_t>(req->count);
        object_types[i] = alloc_array<hxhim_type_t>(req->count);
        objects[i] = alloc_array<void *>(req->count);
        object_lens[i] = alloc_array<std::size_t>(req->count);
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
    Transport::Response::BPut *res = construct<Transport::Response::BPut>(req->count);
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

            destruct(response);
        }
    }

    // clean up each datastore's input array
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        dealloc_array(subjects[i], counters[i]);
        dealloc_array(subject_lens[i], counters[i]);
        dealloc_array(predicates[i], counters[i]);
        dealloc_array(predicate_lens[i], counters[i]);
        dealloc_array(object_types[i], counters[i]);
        dealloc_array(objects[i], counters[i]);
        dealloc_array(object_lens[i], counters[i]);
    }
    dealloc_array(subjects, hx->p->datastore.count);
    dealloc_array(subject_lens, hx->p->datastore.count);
    dealloc_array(predicates, hx->p->datastore.count);
    dealloc_array(predicate_lens, hx->p->datastore.count);
    dealloc_array(object_types, hx->p->datastore.count);
    dealloc_array(objects, hx->p->datastore.count);
    dealloc_array(object_lens, hx->p->datastore.count);
    dealloc_array(counters, hx->p->datastore.count);

    mlog(HXHIM_SERVER_INFO, "Range server BPUT completed");
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
    void ***subjects = alloc_array<void **>(hx->p->datastore.count);
    std::size_t **subject_lens = alloc_array<std::size_t *>(hx->p->datastore.count);
    void ***predicates = alloc_array<void **>(hx->p->datastore.count);
    std::size_t **predicate_lens = alloc_array<std::size_t *>(hx->p->datastore.count);
    hxhim_type_t **object_types = alloc_array<hxhim_type_t *>(hx->p->datastore.count);
    std::size_t *counters = alloc_array<std::size_t>(hx->p->datastore.count);

    // overallocate in case all of the requests go to one datastore
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        subjects[i] = alloc_array<void *>(req->count);
        subject_lens[i] = alloc_array<std::size_t>(req->count);
        predicates[i] = alloc_array<void *>(req->count);
        predicate_lens[i] = alloc_array<std::size_t>(req->count);
        object_types[i] = alloc_array<hxhim_type_t>(req->count);
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

    Transport::Response::BGet *res = construct<Transport::Response::BGet>(req->count);
    res->src = req->dst;
    res->dst = req->src;

    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        Transport::Response::BGet *response = hx->p->datastore.datastores[i]->BGet(subjects[i], subject_lens[i],
                                                                                   predicates[i], predicate_lens[i],
                                                                                   object_types[i],
                                                                                   counters[i]);
        if (response) {
            for(std::size_t j = 0; j < response->count; j++) {
                size_t &count = res->count;
                res->ds_offsets[count] = i;
                res->statuses[count] = response->statuses[j];

                std::swap(res->subjects[count], response->subjects[j]);
                res->subject_lens[count] = response->subject_lens[j];

                std::swap(res->predicates[count], response->predicates[j]);
                res->predicate_lens[count] = response->predicate_lens[j];

                std::swap(res->object_types[count], response->object_types[j]);
                if (res->statuses[count] == HXHIM_SUCCESS) {
                    std::swap(res->objects[count], response->objects[j]);
                    res->object_lens[count] = response->object_lens[j];
                }
                count++;
            }
            destruct(response);
        }
    }

    // clean up each datastore's input array
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        dealloc_array(subjects[i], counters[i]);
        dealloc_array(subject_lens[i], counters[i]);
        dealloc_array(predicates[i], counters[i]);
        dealloc_array(predicate_lens[i], counters[i]);
        dealloc_array(object_types[i], counters[i]);
    }
    dealloc_array(subjects, hx->p->datastore.count);
    dealloc_array(subject_lens, hx->p->datastore.count);
    dealloc_array(predicates, hx->p->datastore.count);
    dealloc_array(predicate_lens, hx->p->datastore.count);
    dealloc_array(object_types, hx->p->datastore.count);
    dealloc_array(counters, hx->p->datastore.count);

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
static Transport::Response::BGet2 *bget2(hxhim_t *hx, const Transport::Request::BGet2 *req) {
    // allocate arrays for each datastore
    void ***subjects = alloc_array<void **>(hx->p->datastore.count);
    std::size_t **subject_lens = alloc_array<std::size_t *>(hx->p->datastore.count);
    void ***predicates = alloc_array<void **>(hx->p->datastore.count);
    std::size_t **predicate_lens = alloc_array<std::size_t *>(hx->p->datastore.count);
    hxhim_type_t **object_types = alloc_array<hxhim_type_t *>(hx->p->datastore.count);
    void ***objects = alloc_array<void **>(hx->p->datastore.count);
    std::size_t ***object_lens = alloc_array<std::size_t **>(hx->p->datastore.count);
    void ***src_objects = alloc_array<void **>(hx->p->datastore.count);
    std::size_t ***src_object_lens = alloc_array<std::size_t **>(hx->p->datastore.count);
    std::size_t *counters = alloc_array<std::size_t>(hx->p->datastore.count);

    // each datastore array is overallocated in case
    // all of the requests go to one datastore
    for(std::size_t ds = 0; ds < hx->p->datastore.count; ds++) {
        subjects[ds] = alloc_array<void *>(req->count);
        subject_lens[ds] = alloc_array<std::size_t>(req->count);
        predicates[ds] = alloc_array<void *>(req->count);
        predicate_lens[ds] = alloc_array<std::size_t>(req->count);
        object_types[ds] = alloc_array<hxhim_type_t>(req->count);
        objects[ds] = alloc_array<void *>(req->count);
        object_lens[ds] = alloc_array<std::size_t *>(req->count);
        src_objects[ds] = alloc_array<void *>(req->count);
        src_object_lens[ds] = alloc_array<std::size_t *>(req->count);
        counters[ds] = 0;
    }

    // split up requests into specific datastores
    for(std::size_t i = 0; i < req->count; i++) {
        const int ds = req->ds_offsets[i];
        std::size_t &index = counters[req->ds_offsets[i]];

        subjects[ds][index] = req->subjects[i];
        subject_lens[ds][index] = req->subject_lens[i];

        predicates[ds][index] = req->predicates[i];
        predicate_lens[ds][index] = req->predicate_lens[i];

        object_types[ds][index] = req->object_types[i];
        objects[ds][index] = req->objects[i];
        object_lens[ds][index] = req->object_lens[i];

        src_objects[ds][index] = req->src_objects[i];
        src_object_lens[ds][index] = req->src_object_lens[i];

        index++;
    }

    // returned responses
    Transport::Response::BGet2 *res = construct<Transport::Response::BGet2>(req->count);
    res->src = req->dst;
    res->dst = req->src;

    for(std::size_t ds = 0; ds < hx->p->datastore.count; ds++) {
        Transport::Response::BGet2 *response = hx->p->datastore.datastores[ds]->BGet2(&subjects[ds], &subject_lens[ds],
                                                                                      &predicates[ds], &predicate_lens[ds],
                                                                                      &object_types[ds], &objects[ds], &object_lens[ds],
                                                                                      &src_objects[ds], &src_object_lens[ds],
                                                                                      counters[ds]);

        if (response) {
            for(std::size_t j = 0; j < response->count; j++) {
                size_t &count = res->count;
                res->ds_offsets[count] = ds;
                res->statuses[count] = response->statuses[j];

                std::swap(res->subjects[count], response->subjects[j]);
                response->subjects[j] = nullptr;
                res->subject_lens[count] = response->subject_lens[j];

                std::swap(res->predicates[count], response->predicates[j]);
                response->predicates[j] = nullptr;
                res->predicate_lens[count] = response->predicate_lens[j];

                std::swap(res->object_types[count], response->object_types[j]);
                if (res->statuses[count] == HXHIM_SUCCESS) {
                    std::swap(res->objects[count], response->objects[j]);
                    response->objects[j] = nullptr;
                    res->object_lens[count] = response->object_lens[j];
                }

                std::swap(res->src_objects[count], response->src_objects[j]);
                response->src_objects[j] = nullptr;
                res->src_object_lens[count] = response->src_object_lens[j];

                count++;
            }
            destruct(response);
        }
    }

    // don't clean up each datastore's arrays
    // they were cleaned up by responses

    dealloc_array(subjects, hx->p->datastore.count);
    dealloc_array(subject_lens, hx->p->datastore.count);
    dealloc_array(predicates, hx->p->datastore.count);
    dealloc_array(predicate_lens, hx->p->datastore.count);
    dealloc_array(object_types, hx->p->datastore.count);
    dealloc_array(objects, hx->p->datastore.count);
    dealloc_array(object_lens, hx->p->datastore.count);
    dealloc_array(src_objects, hx->p->datastore.count);
    dealloc_array(src_object_lens, hx->p->datastore.count);
    dealloc_array(counters, hx->p->datastore.count);

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
    Transport::Response::BGetOp *res = construct<Transport::Response::BGetOp>(req->count);
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

            destruct(response);
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
    void ***subjects = alloc_array<void **>(hx->p->datastore.count);
    std::size_t **subject_lens = alloc_array<std::size_t *>(hx->p->datastore.count);
    void ***predicates = alloc_array<void **>(hx->p->datastore.count);
    std::size_t **predicate_lens = alloc_array<std::size_t *>(hx->p->datastore.count);
    std::size_t *counters = alloc_array<std::size_t>(hx->p->datastore.count);

    // overallocate in case all of the requests go to one datastore
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        subjects[i] = alloc_array<void *>(req->count);
        subject_lens[i] = alloc_array<std::size_t>(req->count);
        predicates[i] = alloc_array<void *>(req->count);
        predicate_lens[i] = alloc_array<std::size_t>(req->count);
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

    Transport::Response::BDelete *res = construct<Transport::Response::BDelete>(req->count);
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

            destruct(response);
        }
    }

    // clean up each datastore's input array
    for(std::size_t i = 0; i < hx->p->datastore.count; i++) {
        dealloc_array(subjects[i], counters[i]);
        dealloc_array(subject_lens[i], counters[i]);
        dealloc_array(predicates[i], counters[i]);
        dealloc_array(predicate_lens[i], counters[i]);
    }
    dealloc_array(subjects, hx->p->datastore.count);
    dealloc_array(subject_lens, hx->p->datastore.count);
    dealloc_array(predicates, hx->p->datastore.count);
    dealloc_array(predicate_lens, hx->p->datastore.count);
    dealloc_array(counters, hx->p->datastore.count);

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
    Transport::Response::BHistogram *ret = construct<Transport::Response::BHistogram>(bhist->count);
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
            destruct(res);
        }
        else {
            ret->statuses[i] = HXHIM_ERROR;
        }

        ret->count++;
    }

    return ret;
}

Transport::Response::Response *range_server(hxhim_t *hx, const Transport::Request::Request *req) {
    mlog(HXHIM_SERVER_INFO, "Range server started");
    using namespace Transport;

    Response::Response *res = nullptr;

    // Call the appropriate function depending on the message type
    switch(req->type) {
        case Message::BPUT:
            mlog(HXHIM_SERVER_INFO, "Range server got a BPUT");
            res = bput(hx, dynamic_cast<const Request::BPut *>(req));
            break;
        case Message::BGET:
            mlog(HXHIM_SERVER_INFO, "Range server got a BGET");
            res = bget(hx, dynamic_cast<const Request::BGet *>(req));
            break;
        case Message::BGET2:
            mlog(HXHIM_SERVER_INFO, "Range server got a BGET2");
            res = bget2(hx, dynamic_cast<const Request::BGet2 *>(req));
            break;
        case Message::BGETOP:
            mlog(HXHIM_SERVER_INFO, "Range server got a BGETOP");
            res = bgetop(hx, dynamic_cast<const Request::BGetOp *>(req));
            break;
        case Message::BDELETE:
            mlog(HXHIM_SERVER_INFO, "Range server got a BDELETE");
            res = bdelete(hx, dynamic_cast<const Request::BDelete *>(req));
            break;
        case Message::HISTOGRAM:
            mlog(HXHIM_SERVER_INFO, "Range server got a HISTOGRAM");
            res = histogram(hx, dynamic_cast<const Request::Histogram *>(req));
            break;
        case Message::BHISTOGRAM:
            mlog(HXHIM_SERVER_INFO, "Range server got a BHISTOGRAM");
            res = bhistogram(hx, dynamic_cast<const Request::BHistogram *>(req));
            break;
        default:
            break;
    }

    mlog(HXHIM_SERVER_INFO, "Range server stopping");
    return res;
}

}
}
