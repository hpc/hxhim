#include <cstdlib>
#include <cstring>

#include "datastore/datastores.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/Results_private.hpp"
#include "hxhim/accessors.hpp"
#include "transport/Messages/Messages.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

hxhim::Results::Result::Result(hxhim_t *hx, const hxhim_result_type type,
                               const int datastore, const int status)
    : hx(hx),
      type(type),
      datastore(datastore),
      status(status)
{}

hxhim::Results::Result::~Result() {}

hxhim::Results::SubjectPredicate::SubjectPredicate(hxhim_t *hx, const hxhim_result_type type,
                                                   const int datastore, const int status)
    : Result(hx, type, datastore, status),
      subject(nullptr),
      predicate(nullptr)
{}

hxhim::Results::SubjectPredicate::~SubjectPredicate() {
    destruct(subject);
    destruct(predicate);
}

hxhim::Results::Put::Put(hxhim_t *hx,
                         const int datastore, const int status)
    : SubjectPredicate(hx, hxhim_result_type::HXHIM_RESULT_PUT, datastore, status)
{}

hxhim::Results::Get::Get(hxhim_t *hx,
                         const int datastore, const int status)
    : SubjectPredicate(hx, hxhim_result_type::HXHIM_RESULT_GET, datastore, status),
      object_type(HXHIM_INVALID_TYPE),
      object(nullptr),
      next(nullptr)
{}

hxhim::Results::GetOp::GetOp(hxhim_t *hx,
                             const int datastore, const int status)
    : SubjectPredicate(hx, hxhim_result_type::HXHIM_RESULT_GETOP, datastore, status),
      object_type(HXHIM_INVALID_TYPE),
      object(nullptr),
      next(nullptr)
{}

hxhim::Results::Delete::Delete(hxhim_t *hx,
                               const int datastore, const int status)
    : SubjectPredicate(hx, hxhim_result_type::HXHIM_RESULT_DEL, datastore, status)
{}

hxhim::Results::Sync::Sync(hxhim_t *hx,
                         const int datastore, const int status)
    : Result(hx, hxhim_result_type::HXHIM_RESULT_SYNC, datastore, status)
{}

hxhim::Results::Histogram::Histogram(hxhim_t *hx,
                         const int datastore, const int status)
    : Result(hx, hxhim_result_type::HXHIM_RESULT_HISTOGRAM, datastore, status)
{}

hxhim::Results::Result *hxhim::Result::init(hxhim_t *hx, Transport::Response::Response *res, const std::size_t i) {
    // hx should have been checked earlier

    int rank = -1;
    hxhim::GetMPIRank(hx, &rank);

    mlog(HXHIM_CLIENT_DBG, "Rank %d Creating hxhim::Results::Result using %p[%zu]", rank, res, i);

    if (!res) {
        mlog(HXHIM_CLIENT_WARN, "Rank %d Could not extract result at index %zu from %p", rank, i, res);
        return nullptr;
    }

    if (i >= res->count) {
        mlog(HXHIM_CLIENT_WARN, "Rank %d Could not extract result at index %zu from %p which has %zu items", rank, i, res, res->count);
        return nullptr;
    }

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
    mlog(HXHIM_CLIENT_DBG, "Rank %d Created hxhim::Results::Result %p using %p[%zu]", rank, ret, res, i);

    return ret;
}

hxhim::Results::Put *hxhim::Result::init(hxhim_t *hx, Transport::Response::BPut *bput, const std::size_t i) {
    hxhim::Results::Put *out = construct<hxhim::Results::Put>(hx, hxhim::datastore::get_id(hx, bput->src, bput->ds_offsets[i]), bput->statuses[i]);

    out->subject = bput->orig.subjects[i];
    out->predicate = bput->orig.predicates[i];

    bput->orig.subjects[i] = nullptr;
    bput->orig.predicates[i] = nullptr;
    return out;
}

hxhim::Results::Get *hxhim::Result::init(hxhim_t *hx, Transport::Response::BGet *bget, const std::size_t i) {
    hxhim::Results::Get *out = construct<hxhim::Results::Get>(hx, hxhim::datastore::get_id(hx, bget->src, bget->ds_offsets[i]), bget->statuses[i]);

    out->subject = bget->orig.subjects[i];
    out->predicate = bget->orig.predicates[i];
    out->object_type = bget->object_types[i];
    out->object = bget->objects[i];
    out->next = nullptr;

    bget->objects[i] = nullptr;
    bget->orig.subjects[i] = nullptr;
    bget->orig.predicates[i] = nullptr;
    return out;
}

hxhim::Results::GetOp *hxhim::Result::init(hxhim_t *hx, Transport::Response::BGetOp *bgetop, const std::size_t i) {
    const int id = hxhim::datastore::get_id(hx, bgetop->src, bgetop->ds_offsets[i]);
    const int status = bgetop->statuses[i];

    hxhim::Results::GetOp *top = construct<hxhim::Results::GetOp>(hx, id, status);

    hxhim::Results::GetOp *prev = nullptr;
    hxhim::Results::GetOp *curr = top;
    for(std::size_t j = 0; i < bgetop->num_recs[i]; j++) {
        curr->object_type      = bgetop->object_types[i];

        curr->subject          = bgetop->subjects[i][j];
        curr->predicate        = bgetop->predicates[i][j];
        curr->object           = bgetop->objects[i][j];

        bgetop->subjects[i][j]   = nullptr;
        bgetop->predicates[i][j] = nullptr;
        bgetop->objects[i][j]    = nullptr;

        prev = curr;
        curr = construct<hxhim::Results::GetOp>(hx, id, status);
        prev->next = curr;
    }

    // drop last item (empty)
    prev->next = nullptr;
    destruct(curr);

    return top;
}

hxhim::Results::Delete *hxhim::Result::init(hxhim_t *hx, Transport::Response::BDelete *bdel, const std::size_t i) {
    hxhim::Results::Delete *out = construct<hxhim::Results::Delete>(hx, hxhim::datastore::get_id(hx, bdel->src, bdel->ds_offsets[i]), bdel->statuses[i]);

    out->subject = bdel->orig.subjects[i];
    out->predicate = bdel->orig.predicates[i];

    bdel->orig.subjects[i] = nullptr;
    bdel->orig.predicates[i] = nullptr;
    return out;
}

hxhim::Results::Sync *hxhim::Result::init(hxhim_t *hx, const int ds_offset, const int synced) {
    int rank = -1;
    hxhim::GetMPIRank(hx, &rank);

    hxhim::Results::Sync *out = construct<hxhim::Results::Sync>(hx, hxhim::datastore::get_id(hx, rank, ds_offset), synced);
    return out;
}

hxhim::Results::Histogram *hxhim::Result::init(hxhim_t *hx, Transport::Response::BHistogram *bhist, const std::size_t i) {
    hxhim::Results::Histogram *out = construct<hxhim::Results::Histogram>(hx, hxhim::datastore::get_id(hx, bhist->src, bhist->ds_offsets[i]), bhist->statuses[i]);

    out->buckets = bhist->hists[i].buckets;
    out->counts = bhist->hists[i].counts;
    out->size = bhist->hists[i].size;
    return out;
}

hxhim::Results::Results(hxhim_t *hx)
    : hx(hx),
      results(),
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
    if (res) {
        int rank = -1;
        hxhim::GetMPIRank(res->hx, &rank);

        mlog(HXHIM_CLIENT_DBG, "Rank %d Destroying hxhim::Results %p", rank, res);
        destruct(res);
        mlog(HXHIM_CLIENT_DBG, "Rank %d Destroyed hxhim:Results %p", rank, res);
    }
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
        // serialize GetOps
        if (res->type == hxhim_result_type_t::HXHIM_RESULT_GETOP) {
            hxhim::Results::GetOp *get = static_cast<hxhim::Results::GetOp *>(res);
            while (get) {
                results.push_back(get);

                hxhim::Results::GetOp *curr = get;
                get = curr->next;
                curr->next = nullptr;
            }
        }
        else {
            results.push_back(res);
        }
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
hxhim_results_t *hxhim_results_init(hxhim_t * hx, hxhim::Results *res) {
    int rank = -1;
    hxhim::GetMPIRank(hx, &rank);

    mlog(HXHIM_CLIENT_DBG, "Rank %d Creating hxhim_results_t using %p", rank, res);
    hxhim_results_t *ret = construct<hxhim_results_t>();
    ret->hx = hx;
    ret->res = res;
    mlog(HXHIM_CLIENT_DBG, "Rank %d Created hxhim_results_t %p with %p inside", rank, ret, res);
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

    int rc = HXHIM_ERROR;

    hxhim::Results::Result *curr = res->res->Curr();
    switch (curr->type) {
        case hxhim_result_type::HXHIM_RESULT_PUT:
        case hxhim_result_type::HXHIM_RESULT_GET:
        case hxhim_result_type::HXHIM_RESULT_DEL:
            {
                hxhim::Results::SubjectPredicate *sp = static_cast<hxhim::Results::SubjectPredicate *>(curr);

                if (subject) {
                    *subject = sp->subject->ptr;
                }

                if (subject_len) {
                    *subject_len = sp->subject->len;
                }
            }
            break;
        default:
            break;
    }

    return rc;
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

    int rc = HXHIM_ERROR;

    hxhim::Results::Result *curr = res->res->Curr();
    switch (curr->type) {
        case hxhim_result_type::HXHIM_RESULT_PUT:
        case hxhim_result_type::HXHIM_RESULT_GET:
        case hxhim_result_type::HXHIM_RESULT_DEL:
            {
                hxhim::Results::SubjectPredicate *sp = static_cast<hxhim::Results::SubjectPredicate *>(curr);

                if (predicate) {
                    *predicate = sp->predicate->ptr;
                }

                if (predicate_len) {
                    *predicate_len = sp->predicate->len;
                }
            }
            break;
        default:
            break;
    }

    return rc;
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
int hxhim_results_get_object(hxhim_results_t *res, void **object, size_t *object_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *curr = res->res->Curr();
    if (curr->type == hxhim_result_type::HXHIM_RESULT_GET) {
        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(curr);

        if (object) {
            *object = get->object->ptr;
        }

        if (object_len) {
            *object_len = get->object->len;
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
void hxhim_results_destroy(hxhim_results_t *res) {
    if (res) {
        int rank = -1;
        hxhim::GetMPIRank(res->hx, &rank);

        mlog(HXHIM_CLIENT_DBG, "Rank %d Destroying hxhim_results_t %p", rank, res);
        hxhim::Results::Destroy(res->res);
        destruct(res);
        mlog(HXHIM_CLIENT_DBG, "Rank %d Destroyed hxhim_results_t %p", rank, res);
    }
}
