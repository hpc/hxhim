#include <cstdlib>
#include <cstring>

#include "hxhim/Results.hpp"
#include "hxhim/Results_private.hpp"
#include "hxhim/private.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace hxhim {

Results::Result::~Result() {}

void Result::destroy(hxhim_t *hx, Results::Result *res) {
    hx->p->memory_pools.result->release(res);
}

Results::Get::~Get() {
    // buffers->release(subject, subject_len);
    // buffers->release(predicate, predicate_len);
    buffers->release(object, object_len);
}

Results::Get2::~Get2() {
    buffers->release(subject, subject_len);
    buffers->release(predicate, predicate_len);
    // object and object_len came from user space
}

Results::Put *Result::init(hxhim_t *hx, Transport::Response::Put *put) {
    if (!valid(hx) || !put) {
        return nullptr;
    }

    Results::Put *out = hx->p->memory_pools.result->acquire<Results::Put>();
    out->type = hxhim_result_type::HXHIM_RESULT_PUT;
    out->buffers = hx->p->memory_pools.buffers;
    out->datastore = hxhim::datastore::get_id(hx, put->src, put->ds_offset);
    out->status = put->status;
    return out;
}

Results::Put *Result::init(hxhim_t *hx, Transport::Response::BPut *bput, const std::size_t i) {
    if (!valid(hx) || !bput || (i >= bput->count)) {
        return nullptr;
    }

    Results::Put *out = hx->p->memory_pools.result->acquire<Results::Put>();
    out->type = hxhim_result_type::HXHIM_RESULT_PUT;
    out->buffers = hx->p->memory_pools.buffers;
    out->datastore = hxhim::datastore::get_id(hx, bput->src, bput->ds_offsets[i]);
    out->status = bput->statuses[i];
    return out;
}

void Result::destroy(hxhim_t *hx, Results::Put *put) {
    if (!valid(hx)) {
        return;
    }

    hx->p->memory_pools.result->release(put);
}

Results::Get *Result::init(hxhim_t *hx, Transport::Response::Get *get) {
    if (!valid(hx) || !get) {
        return nullptr;
    }

    Results::Get *out = hx->p->memory_pools.result->acquire<Results::Get>();
    out->type = hxhim_result_type::HXHIM_RESULT_GET;
    out->buffers = hx->p->memory_pools.buffers;
    out->datastore = hxhim::datastore::get_id(hx, get->src, get->ds_offset);
    out->status = get->status;
    out->subject = get->subject;
    out->subject_len = get->subject_len;
    out->predicate = get->predicate;
    out->predicate_len = get->predicate_len;
    out->object_type = get->object_type;
    out->object = get->object;
    out->object_len = get->object_len;

    get->subject = nullptr;
    get->predicate = nullptr;
    get->object = nullptr;
    return out;
}

Results::Get2 *Result::init(hxhim_t *hx, Transport::Response::Get2 *get) {
    if (!valid(hx) || !get) {
        return nullptr;
    }

    Results::Get2 *out = hx->p->memory_pools.result->acquire<Results::Get2>();
    out->type = hxhim_result_type::HXHIM_RESULT_GET2;
    out->buffers = hx->p->memory_pools.buffers;
    out->datastore = hxhim::datastore::get_id(hx, get->src, get->ds_offset);
    out->status = get->status;

    get->subject = nullptr;
    get->predicate = nullptr;
    get->object = nullptr;
    return out;
}

Results::Get *Result::init(hxhim_t *hx, Transport::Response::BGet *bget, const std::size_t i) {
    if (!valid(hx) || !bget || (i >= bget->count)) {
        return nullptr;
    }

    Results::Get *out = hx->p->memory_pools.result->acquire<Results::Get>();
    out->type = hxhim_result_type::HXHIM_RESULT_GET;
    out->buffers = hx->p->memory_pools.buffers;
    out->datastore = hxhim::datastore::get_id(hx, bget->src, bget->ds_offsets[i]);
    out->status = bget->statuses[i];
    out->subject = bget->subjects[i];
    out->subject_len = bget->subject_lens[i];
    out->predicate = bget->predicates[i];
    out->predicate_len = bget->predicate_lens[i];
    out->object_type = bget->object_types[i];
    out->object = bget->objects[i];
    out->object_len = bget->object_lens[i];

    bget->subjects[i] = nullptr;
    bget->predicates[i] = nullptr;
    bget->objects[i] = nullptr;
    return out;
}

Results::Get2 *Result::init(hxhim_t *hx, Transport::Response::BGet2 *bget, const std::size_t i) {
    if (!valid(hx) || !bget || (i >= bget->count)) {
        return nullptr;
    }

    Results::Get2 *out = hx->p->memory_pools.result->acquire<Results::Get2>();
    out->type = hxhim_result_type::HXHIM_RESULT_GET2;
    out->buffers = hx->p->memory_pools.buffers;
    out->datastore = hxhim::datastore::get_id(hx, bget->src, bget->ds_offsets[i]);
    out->status = bget->statuses[i];
    out->subject = bget->subjects[i];
    out->subject_len = bget->subject_lens[i];
    out->predicate = bget->predicates[i];
    out->predicate_len = bget->predicate_lens[i];
    out->object_type = bget->object_types[i];
    out->object = bget->objects[i];
    out->object_len = bget->object_lens[i];

    bget->subjects[i] = nullptr;
    bget->predicates[i] = nullptr;
    bget->objects[i] = nullptr;
    return out;
}

Results::Get *Result::init(hxhim_t *hx, Transport::Response::BGetOp *bgetop, const std::size_t i) {
    if (!valid(hx) || !bgetop || (i >= bgetop->count)) {
        return nullptr;
    }

    Results::Get *out = hx->p->memory_pools.result->acquire<Results::Get>();
    out->type = hxhim_result_type::HXHIM_RESULT_GET;
    out->buffers = hx->p->memory_pools.buffers;
    out->datastore = hxhim::datastore::get_id(hx, bgetop->src, bgetop->ds_offsets[i]);
    out->status = bgetop->statuses[i];
    out->subject = bgetop->subjects[i];
    out->subject_len = bgetop->subject_lens[i];
    out->predicate = bgetop->predicates[i];
    out->predicate_len = bgetop->predicate_lens[i];
    out->object_type = bgetop->object_types[i];
    out->object = bgetop->objects[i];
    out->object_len = bgetop->object_lens[i];

    bgetop->subjects[i] = nullptr;
    bgetop->predicates[i] = nullptr;
    bgetop->objects[i] = nullptr;
    return out;
}

void Result::destroy(hxhim_t *hx, Results::Get *get) {
    if (!valid(hx)) {
        return;
    }

    hx->p->memory_pools.result->release(get);
}

void Result::destroy(hxhim_t *hx, Results::Get2 *get) {
    if (!valid(hx)) {
        return;
    }

    hx->p->memory_pools.result->release(get);
}

Results::Delete *Result::init(hxhim_t *hx, Transport::Response::Delete *del) {
    if (!valid(hx) || !del) {
        return nullptr;
    }

    Results::Delete *out = hx->p->memory_pools.result->acquire<Results::Delete>();
    out->type = hxhim_result_type::HXHIM_RESULT_DEL;
    out->buffers = hx->p->memory_pools.buffers;
    out->datastore = hxhim::datastore::get_id(hx, del->src, del->ds_offset);
    out->status = del->status;
    return out;
}

Results::Delete *Result::init(hxhim_t *hx, Transport::Response::BDelete *bdel, const std::size_t i) {
    if (!valid(hx) || !bdel || (i >= bdel->count)) {
        return nullptr;
    }

    Results::Delete *out = hx->p->memory_pools.result->acquire<Results::Delete>();
    out->type = hxhim_result_type::HXHIM_RESULT_DEL;
    out->buffers = hx->p->memory_pools.buffers;
    out->datastore = hxhim::datastore::get_id(hx, bdel->src, bdel->ds_offsets[i]);
    out->status = bdel->statuses[i];
    return out;
}

void Result::destroy(hxhim_t *hx, Results::Delete *del) {
    if (!valid(hx)) {
        return;
    }

    hx->p->memory_pools.result->release(del);
}

Results::Sync *Result::init(hxhim_t *hx, const int ds_offset, const int synced) {
    if (!valid(hx)) {
        return nullptr;
    }

    Results::Sync *out = hx->p->memory_pools.result->acquire<Results::Sync>();
    out->type = hxhim_result_type::HXHIM_RESULT_SYNC;
    out->buffers = hx->p->memory_pools.buffers;
    out->datastore = hxhim::datastore::get_id(hx, hx->p->bootstrap.rank, ds_offset);
    out->status = synced;
    return out;
}

void Result::destroy(hxhim_t *hx, Results::Sync *sync) {
    if (!valid(hx)) {
        return;
    }

    hx->p->memory_pools.result->release(sync);
}

Results::Histogram *Result::init(hxhim_t *hx, Transport::Response::Histogram *hist) {
    if (!valid(hx) || !hist) {
        return nullptr;
    }

    Results::Histogram *out = hx->p->memory_pools.result->acquire<Results::Histogram>();
    out->type = hxhim_result_type::HXHIM_RESULT_HISTOGRAM;
    out->buffers = hx->p->memory_pools.buffers;
    out->datastore = hxhim::datastore::get_id(hx, hist->src, hist->ds_offset);
    out->buckets = hist->hist.buckets;
    hist->hist.buckets = nullptr;
    out->counts = hist->hist.counts;
    hist->hist.counts = nullptr;
    out->size = hist->hist.size;
    hist->hist.size = 0;
    out->status = hist->status;
    return out;
}

Results::Histogram *Result::init(hxhim_t *hx, Transport::Response::BHistogram *bhist, const std::size_t i) {
    if (!valid(hx) || !bhist || (i >= bhist->count)) {
        return nullptr;
    }

    Results::Histogram *out = hx->p->memory_pools.result->acquire<Results::Histogram>();
    out->type = hxhim_result_type::HXHIM_RESULT_HISTOGRAM;
    out->buffers = hx->p->memory_pools.buffers;
    out->datastore = hxhim::datastore::get_id(hx, bhist->src, bhist->ds_offsets[i]);
    out->buckets = bhist->hists[i].buckets;
    out->counts = bhist->hists[i].counts;
    out->size = bhist->hists[i].size;
    out->status = bhist->statuses[i];
    return out;
}

void Result::destroy(hxhim_t *hx, Results::Histogram *hist) {
    if (!valid(hx)) {
        return;
    }

    hx->p->memory_pools.result->release(hist);
}

Results::Results(hxhim_t *hx)
    : hx(hx),
      results(),
      curr(results.end())
{}

Results::~Results() {
    curr = results.end();

    for(Result *res : results) {
        switch (res->type) {
            case HXHIM_RESULT_PUT:
                hxhim::Result::destroy(hx, static_cast<Put *>(res));
                break;
            case HXHIM_RESULT_GET:
                hxhim::Result::destroy(hx, static_cast<Get *>(res));
                break;
            case HXHIM_RESULT_GET2:
                hxhim::Result::destroy(hx, static_cast<Get2 *>(res));
                break;
            case HXHIM_RESULT_DEL:
                hxhim::Result::destroy(hx, static_cast<Delete *>(res));
                break;
            case HXHIM_RESULT_SYNC:
                hxhim::Result::destroy(hx, static_cast<Sync *>(res));
                break;
            case HXHIM_RESULT_HISTOGRAM:
                hxhim::Result::destroy(hx, static_cast<Histogram *>(res));
                break;
            default:
                hxhim::Result::destroy(hx, res);
                break;
        }
    }

    results.clear();
}

/**
 * Destroy
 * Destroys the results returned from an operation
 *
 * @param hx    the HXHIM session
 * @param res   pointer to a set of results
 */
void Results::Destroy(hxhim_t *hx, Results *res) {
    mlog(HXHIM_CLIENT_DBG, "Destroying hxhim::Results %p", res);
    hx->p->memory_pools.results->release(res);
    mlog(HXHIM_CLIENT_DBG, "Destroyed hxhim:Results %p", res);
}

/**
 * Valid
 *
 * @return Whether or not the current node is a valid pointer
 */
bool Results::Valid() const {
    return (curr != results.end());
}

/**
 * GoToHead
 * Moves the current node to point to the head of the list
 *
 * @return the pointer to the head of the list
 */
Results::Result *Results::GoToHead() {
    curr = results.begin();
    return Valid()?*curr:nullptr;;
}

/**
 * GoToNext
 * Moves the current node to point to the next node in the list
 *
 * @return the pointer to the next node in the list
 */
Results::Result *Results::GoToNext() {
    if (Valid()) {
        ++curr;
    }
    return Valid()?*curr:nullptr;;
}

/**
 * Curr
 *
 * @return the pointer to the node currently being pointed to
 */
Results::Result *Results::Curr() const {
    return Valid()?*curr:nullptr;;
}

/**
 * Curr
 *
 * @return the pointer to the node after the one currently being pointed to
 */
Results::Result *Results::Next() const {
    std::list<Result *>::iterator it = std::next(curr);
    return (it != results.end())?*it:nullptr;;
}

/**
 * Add
 * Appends a single result node to the end of the list;
 *
 * @return the pointer to the last valid node
 */
Results::Result *Results::Add(Results::Result *res) {
    if (res) {
        results.push_back(res);
    }
    std::list<Result *>::reverse_iterator it = results.rbegin();
    return (it != results.rend())?*it:nullptr;
}

/**
 * Append
 * Moves and appends the contents of another hxhim::Results into this one.
 * The other list is emptied out;
 *
 * @return the pointer to the new node
 */
Results &Results::Append(Results *other) {
    if (other) {
        results.splice(results.end(), other->results);
    }

    return *this;
}

std::size_t Results::size() const {
    return results.size();
}

}

/**
 * hxhim_results_init
 *
 * @param res A hxhim::Results instance
 * @return the pointer to the C structure containing the hxhim::Results
 */
hxhim_results_t *hxhim_results_init(hxhim_t *hx, hxhim::Results *res) {
    mlog(HXHIM_CLIENT_DBG, "Creating hxhim_results_t using %p", res);
    hxhim_results_t *ret = hx->p->memory_pools.buffers->acquire<hxhim_results_t>();
    ret->res = res;
    mlog(HXHIM_CLIENT_DBG, "Created hxhim_results_t %p with %p inside", ret, res);
    return ret;
}

/**
 * hxhim_results_goto_head
 * Moves the internal pointer to the head of the list
 *
 * @param res A list of results
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_goto_head(hxhim_results_t *res) {
    if (res && res->res) {
        res->res->GoToHead();
        return HXHIM_SUCCESS;
    }

    return HXHIM_ERROR;
}

/**
 * hxhim_results_goto_next
 * Moves the internal pointer to the next element in the list
 *
 * @param res A list of results
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_goto_next(hxhim_results_t *res) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    res->res->GoToNext();
    return HXHIM_SUCCESS;
}

/**
 * hxhim_results_valid
 *
 * @param res A list of results
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_valid(hxhim_results_t *res) {
    return (res && res->res && res->res->Valid())?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * hxhim_results_type
 * Gets the type of the result node currently being pointed to
 *
 * @param res   A list of results
 * @param type  (optional) the type of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_type(hxhim_results_t *res, enum hxhim_result_type *type) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (!curr) {
        return HXHIM_ERROR;
    }

    if (type) {
        *type = curr->type;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_results_error
 * Gets the error of the result node currently being pointed to
 *
 * @param res    A list of results
 * @param error  (optional) the error of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_error(hxhim_results_t *res, int *error) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (!curr) {
        return HXHIM_ERROR;
    }

    if (error) {
        *error = curr->status;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_results_datastore
 * Gets the datastore of the result node currently being pointed to
 *
 * @param res       A list of results
 * @param datastore  (optional) the datastore of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_datastore(hxhim_results_t *res, int *datastore) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (!curr) {
        return HXHIM_ERROR;
    }

    if (datastore) {
        *datastore = curr->datastore;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_results_get_object_type
 * Gets the object and length from the current result node, if the result node contains data from a GET
 *
 * @param res         A list of results
 * @param object_type (optional) the object type of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_get_object_type(hxhim_results_t *res, hxhim_type_t *object_type) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if ((curr->type == hxhim_result_type::HXHIM_RESULT_GET) || (curr->type == hxhim_result_type::HXHIM_RESULT_GET2)) {
        if (object_type) {
            *object_type = static_cast<hxhim::Results::Get *>(curr)->object_type;
            return HXHIM_SUCCESS;
        }
    }

    return HXHIM_ERROR;
}

/**
 * hxhim_results_get_subject
 * Gets the subject and length from the current result node, if the result node contains data from a GET
 *
 * @param res          A list of results
 * @param subject      (optional) the subject of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param subject_len  (optional) the subject_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_get_subject(hxhim_results_t *res, void **subject, std::size_t *subject_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if ((curr->type == hxhim_result_type::HXHIM_RESULT_GET) || (curr->type == hxhim_result_type::HXHIM_RESULT_GET2)) {
        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(curr);
        if (subject) {
            *subject = get->subject;
        }

        if (subject_len) {
            *subject_len = get->subject_len;
        }

        return HXHIM_SUCCESS;
    }

    return HXHIM_ERROR;
}

/**
 * hxhim_results_get_predicate
 * Gets the predicate and length from the current result node, if the result node contains data from a GET
 *
 * @param res            A list of results
 * @param predicate      (optional) the predicate of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param predicate_len  (optional) the predicate_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_get_predicate(hxhim_results_t *res, void **predicate, std::size_t *predicate_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if ((curr->type == hxhim_result_type::HXHIM_RESULT_GET) || (curr->type == hxhim_result_type::HXHIM_RESULT_GET2)) {
        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(curr);
        if (predicate) {
            *predicate = get->predicate;
        }

        if (predicate_len) {
            *predicate_len = get->predicate_len;
        }

        return HXHIM_SUCCESS;
    }

    return HXHIM_ERROR;
}

/**
 * hxhim_results_get_object
 * Gets the object and length from the current result node, if the result node contains data from a GET
 *
 * @param res         A list of results
 * @param object      (optional) the object of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param object_len  (optional) the object_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_get_object(hxhim_results_t *res, void **object, std::size_t *object_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if ((curr->type == hxhim_result_type::HXHIM_RESULT_GET) || (curr->type == hxhim_result_type::HXHIM_RESULT_GET2)) {
        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(curr);
        if (object) {
            *object = get->object;
        }

        if (object_len) {
            *object_len = get->object_len;
        }

        return HXHIM_SUCCESS;
    }

    return HXHIM_ERROR;
}

/**
 * hxhim_results_destroy
 * Destroys the contents of a results struct and the object it is pointing to
 *
 * @param res A list of results
 */
void hxhim_results_destroy(hxhim_t *hx, hxhim_results_t *res) {
    mlog(HXHIM_CLIENT_DBG, "Destroying hxhim_results_t %p", res);
    if (res) {
        mlog(HXHIM_CLIENT_DBG, "Destroying res->res %p", res->res);
        hxhim::Results::Destroy(hx, res->res);
        hx->p->memory_pools.buffers->release(res);
    }
    mlog(HXHIM_CLIENT_DBG, "Destroyed hxhim_results_t %p", res);
}
