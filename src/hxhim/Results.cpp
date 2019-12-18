#include <cstdlib>
#include <cstring>

#include "hxhim/Results.hpp"
#include "hxhim/Results_private.hpp"
#include "hxhim/private.hpp"
#include "transport/Messages/Messages.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

hxhim::Results::Result::~Result() {}

hxhim::Results::Get::Get()
    : subject(nullptr),
      predicate(nullptr),
      object_type(HXHIM_INVALID_TYPE),
      object(nullptr),
      orig()
{}

hxhim::Results::Get::~Get() {
    destruct(subject);
    destruct(predicate);
    destruct(object);
}

hxhim::Results::Result *hxhim::Result::init(hxhim_t *hx, Transport::Response::Response *res, const std::size_t i) {
    Results::Result *ret = nullptr;
    switch (res->type) {
        case Transport::Message::BPUT:
            ret = init(hx, static_cast<Transport::Response::BPut *>(res), i);
            break;
        case Transport::Message::BGET:
            ret = init(hx, static_cast<Transport::Response::BGet *>(res), i);
            break;
        case Transport::Message::BGETOP:
            ret = init(hx, static_cast<Transport::Response::BGetOp *>(res), i);
            break;
        case Transport::Message::BDELETE:
            ret = init(hx, static_cast<Transport::Response::BDelete *>(res), i);
            break;
        default:
            break;
    }

    return ret;
}

hxhim::Results::Put *hxhim::Result::init(hxhim_t *hx, Transport::Response::BPut *bput, const std::size_t i) {
    if (!valid(hx) || !bput || (i >= bput->count)) {
        return nullptr;
    }

    hxhim::Results::Put *out = construct<hxhim::Results::Put>();
    out->type = hxhim_result_type::HXHIM_RESULT_PUT;
    out->datastore = hxhim::datastore::get_id(hx, bput->src, bput->ds_offsets[i]);
    out->status = bput->statuses[i];
    return out;
}

hxhim::Results::Get *hxhim::Result::init(hxhim_t *hx, Transport::Response::BGet *bget, const std::size_t i) {
    if (!valid(hx) || !bget || (i >= bget->count)) {
        return nullptr;
    }

    hxhim::Results::Get *out = construct<hxhim::Results::Get>();
    out->type = hxhim_result_type::HXHIM_RESULT_GET;
    out->datastore = hxhim::datastore::get_id(hx, bget->src, bget->ds_offsets[i]);
    out->status = bget->statuses[i];
    out->subject = bget->subjects[i];
    out->predicate = bget->predicates[i];
    out->object_type = bget->object_types[i];
    out->object = bget->objects[i];

    out->orig.object = bget->orig.objects[i];
    out->orig.object_len = bget->orig.object_lens[i];

    bget->subjects[i] = nullptr;
    bget->predicates[i] = nullptr;
    bget->objects[i] = nullptr;

    return out;
}

hxhim::Results::Get *hxhim::Result::init(hxhim_t *hx, Transport::Response::BGetOp *bgetop, const std::size_t i) {
    if (!valid(hx) || !bgetop || (i >= bgetop->count)) {
        return nullptr;
    }

    hxhim::Results::Get *out = construct<hxhim::Results::Get>();
    out->type = hxhim_result_type::HXHIM_RESULT_GET;
    out->datastore = hxhim::datastore::get_id(hx, bgetop->src, bgetop->ds_offsets[i]);
    out->status = bgetop->statuses[i];
    out->subject = bgetop->subjects[i];
    out->predicate = bgetop->predicates[i];
    out->object_type = bgetop->object_types[i];
    out->object = bgetop->objects[i];

    bgetop->subjects[i] = nullptr;
    bgetop->predicates[i] = nullptr;
    bgetop->objects[i] = nullptr;
    return out;
}

hxhim::Results::Delete *hxhim::Result::init(hxhim_t *hx, Transport::Response::BDelete *bdel, const std::size_t i) {
    if (!valid(hx) || !bdel || (i >= bdel->count)) {
        return nullptr;
    }

    hxhim::Results::Delete *out = construct<hxhim::Results::Delete>();
    out->type = hxhim_result_type::HXHIM_RESULT_DEL;
    out->datastore = hxhim::datastore::get_id(hx, bdel->src, bdel->ds_offsets[i]);
    out->status = bdel->statuses[i];
    return out;
}

hxhim::Results::Sync *hxhim::Result::init(hxhim_t *hx, const int ds_offset, const int synced) {
    if (!valid(hx)) {
        return nullptr;
    }

    hxhim::Results::Sync *out = construct<hxhim::Results::Sync>();
    out->type = hxhim_result_type::HXHIM_RESULT_SYNC;
    out->datastore = hxhim::datastore::get_id(hx, hx->p->bootstrap.rank, ds_offset);
    out->status = synced;
    return out;
}

hxhim::Results::Histogram *hxhim::Result::init(hxhim_t *hx, Transport::Response::BHistogram *bhist, const std::size_t i) {
    if (!valid(hx) || !bhist || (i >= bhist->count)) {
        return nullptr;
    }

    hxhim::Results::Histogram *out = construct<hxhim::Results::Histogram>();
    out->type = hxhim_result_type::HXHIM_RESULT_HISTOGRAM;
    out->datastore = hxhim::datastore::get_id(hx, bhist->src, bhist->ds_offsets[i]);
    out->buckets = bhist->hists[i].buckets;
    out->counts = bhist->hists[i].counts;
    out->size = bhist->hists[i].size;
    out->status = bhist->statuses[i];
    return out;
}

hxhim::Results::Results()
    : results(),
      curr(results.end())
{}

hxhim::Results::~Results() {
    curr = results.end();

    for(Result *res : results) {
        destruct(res);
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
void hxhim::Results::Destroy(Results *res) {
    mlog(HXHIM_CLIENT_DBG, "Destroying hxhim::Results %p", res);
    destruct(res);
    mlog(HXHIM_CLIENT_DBG, "Destroyed hxhim:Results %p", res);
}

/**
 * Valid
 *
 * @return Whether or not the current node is a valid pointer
 */
bool hxhim::Results::Valid() const {
    return (curr != results.end());
}

/**
 * GoToHead
 * Moves the current node to point to the head of the list
 *
 * @return the pointer to the head of the list
 */
hxhim::Results::Result *hxhim::Results::GoToHead() {
    curr = results.begin();
    return Valid()?*curr:nullptr;;
}

/**
 * GoToNext
 * Moves the current node to point to the next node in the list
 *
 * @return the pointer to the next node in the list
 */
hxhim::Results::Result *hxhim::Results::GoToNext() {
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
hxhim::Results::Result *hxhim::Results::Curr() const {
    return Valid()?*curr:nullptr;;
}

/**
 * Curr
 *
 * @return the pointer to the node after the one currently being pointed to
 */
hxhim::Results::Result *hxhim::Results::Next() const {
    std::list<Result *>::iterator it = std::next(curr);
    return (it != results.end())?*it:nullptr;;
}

/**
 * Add
 * Appends a single result node to the end of the list;
 *
 * @return the pointer to the last valid node
 */
hxhim::Results::Result *hxhim::Results::Add(hxhim::Results::Result *res) {
    if (res) {
        results.push_back(res);
    }
    std::list<hxhim::Results::Result *>::reverse_iterator it = results.rbegin();
    return (it != results.rend())?*it:nullptr;
}

/**
 * Append
 * Moves and appends the contents of another hxhim::Results into this one.
 * The other list is emptied out;
 *
 * @return the pointer to the construct<node
 */
hxhim::Results &hxhim::Results::Append(hxhim::Results *other) {
    if (other) {
        results.splice(results.end(), other->results);
    }

    return *this;
}

std::size_t hxhim::Results::size() const {
    return results.size();
}

/**
 * hxhim_results_init
 *
 * @param res A hxhim::Results instance
 * @return the pointer to the C structure containing the hxhim::Results
 */
hxhim_results_t *hxhim_results_init(hxhim::Results *res) {
    mlog(HXHIM_CLIENT_DBG, "Creating hxhim_results_t using %p", res);
    hxhim_results_t *ret = construct<hxhim_results_t>();
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
    if (curr->type == hxhim_result_type::HXHIM_RESULT_GET) {
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
int hxhim_results_get_subject(hxhim_results_t *res, void **subject, size_t *subject_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (curr->type == hxhim_result_type::HXHIM_RESULT_GET) {

        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(curr);
        if (subject) {
            *subject = get->subject->ptr;
        }

        if (subject_len) {
            *subject_len = get->subject->len;
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
int hxhim_results_get_predicate(hxhim_results_t *res, void **predicate, size_t *predicate_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (curr->type == hxhim_result_type::HXHIM_RESULT_GET) {
        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(curr);
        if (predicate) {
            *predicate = get->predicate->ptr;
        }

        if (predicate_len) {
            *predicate_len = get->predicate->len;
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
int hxhim_results_get_object(hxhim_results_t *res, void **object, size_t **object_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (curr->type == hxhim_result_type::HXHIM_RESULT_GET) {
        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(curr);

        if (object) {
            *object = get->orig.object;
        }
        if (object_len) {
            *object_len = get->orig.object_len;
        }

        // void *orig_object = get->orig.object;
        // if (orig_object) {
        //     memcpy(orig_object, get->object->ptr, get->object->len);
        // }

        // std::size_t *orig_object_len = get->orig.object_len;
        // if (orig_object_len) {
        //     *orig_object_len = get->object->len;
        // }

        // if (object) {
        //     *object = orig_object;
        // }

        // if (object_len) {
        //     *object_len = orig_object_len;
        // }

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
void hxhim_results_destroy(hxhim_results_t *res) {
    mlog(HXHIM_CLIENT_DBG, "Destroying hxhim_results_t %p", res);
    if (res) {
        mlog(HXHIM_CLIENT_DBG, "Destroying res->res %p", res->res);
        hxhim::Results::Destroy(res->res);
        destruct(res);
    }
    mlog(HXHIM_CLIENT_DBG, "Destroyed hxhim_results_t %p", res);
}
