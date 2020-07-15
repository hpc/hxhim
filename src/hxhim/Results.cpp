#include <cstdlib>
#include <cstring>

#include "datastore/datastores.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/private/Results.hpp"
#include "hxhim/private/accessors.hpp"
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
    : hxhim::Results::Result(hx, type, datastore, status),
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

hxhim::Results::Delete::Delete(hxhim_t *hx,
                               const int datastore, const int status)
    : SubjectPredicate(hx, hxhim_result_type::HXHIM_RESULT_DEL, datastore, status)
{}

hxhim::Results::Sync::Sync(hxhim_t *hx,
                           const int datastore, const int status)
    : Result(hx, hxhim_result_type::HXHIM_RESULT_SYNC, datastore, status)
{}

hxhim::Results::Result *hxhim::Result::init(hxhim_t *hx, Transport::Response::Response *res, const std::size_t i) {
    // hx should have been checked earlier

    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_CLIENT_DBG, "Rank %d Creating hxhim::Results::Result using %p[%zu]", rank, res, i);

    if (!res) {
        mlog(HXHIM_CLIENT_WARN, "Rank %d Could not extract result at index %zu from %p", rank, i, res);
        return nullptr;
    }

    if (i >= res->count) {
        mlog(HXHIM_CLIENT_WARN, "Rank %d Could not extract result at index %zu from %p which has %zu items", rank, i, res, res->count);
        return nullptr;
    }

    hxhim::Results::Result *ret = nullptr;
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
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    hxhim::Results::Sync *out = construct<hxhim::Results::Sync>(hx, hxhim::datastore::get_id(hx, rank, ds_offset), synced);
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
        hxhim::nocheck::GetMPI(res->hx, nullptr, &rank, nullptr);

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
 * hxhim_results_valid
 *
 * @param res A list of results
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_valid(hxhim_results_t *res) {
    return (res && res->res && res->res->Valid())?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * GoToHead
 * Moves the current node to point to the head of the list
 *
 * @return Whether or not the new position is valid
 */
hxhim::Results::Result *hxhim::Results::GoToHead() {
    curr = results.begin();
    return Valid()?*curr:nullptr;
}

/**
 * hxhim_results_goto_head
 * Moves the internal pointer to the head of the list
 *
 * @param res A list of results
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_goto_head(hxhim_results_t *res) {
    // cannot use hxhim::Results::Valid here
    // since curr might be pointing anywhere
    if (res && res->res) {
        return res->res->GoToHead()?HXHIM_SUCCESS:HXHIM_ERROR;
    }

    return HXHIM_ERROR;
}

/**
 * GoToNext
 * Moves the current node to point to the next node in the list
 *
 * @return Whether or not the new position is valid
 */
hxhim::Results::Result *hxhim::Results::GoToNext() {
    curr++;
    return Valid()?*curr:nullptr;
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

    return res->res->GoToNext()?HXHIM_SUCCESS:HXHIM_ERROR;
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
 * Add
 * Appends a single result node to the end of the list;
 *
 * @return the pointer to the last valid node
 */
hxhim::Results::Result *hxhim::Results::Add(hxhim::Results::Result *res) {
    if (res) {
        // serialize GetOps
        if (res->type == hxhim_result_type_t::HXHIM_RESULT_GETOP) {
            for(hxhim::Results::GetOp *get = static_cast<hxhim::Results::GetOp *>(res);
                get; get = get->next) {
                results.push_back(get);
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
void hxhim::Results::Append(hxhim::Results *other) {
    if (other) {
        results.splice(results.end(), other->results);
    }
}

/**
 * Size
 * Get the number of elements in this set of results
 *
 * @return number of elements
 */
std::size_t hxhim::Results::Size() const {
    return results.size();
}

/**
 * Size
 * Get the number of elements in this set of results
 *
 * @param res   A list of results
 * @return number of elements
 */
size_t hxhim_results_size(hxhim_results_t *res) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return 0;
    }

    return res->res->Size();
}

/**
 * Type
 * Gets the type of the result node currently being pointed to
 *
 * @param type  (optional) the type of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Type(enum hxhim_result_type *type) const {
    if (!Valid()) {
        return HXHIM_ERROR;
    }

    if (type) {
        *type = Curr()->type;
    }

    return HXHIM_SUCCESS;
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

    return res->res->Type(type);
}

/**
 * Status
 * Gets the status of the result node currently being pointed to
 *
 * @param error  (optional) the error of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Status(int *status) const {
    if (!Valid()) {
        return HXHIM_ERROR;
    }

    if (status) {
        *status = Curr()->status;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_results_status
 * Gets the status of the result node currently being pointed to
 *
 * @param res     A list of results
 * @param status  (optional) the status of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_status(hxhim_results_t *res, int *status) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->Status(status);
}

/**
 * Datastore
 * Gets the datastore of the result node currently being pointed to
 *
 * @param datastore  (optional) the datastore of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Datastore(int *datastore) const {
    if (!Valid()) {
        return HXHIM_ERROR;
    }

    if (datastore) {
        *datastore = Curr()->datastore;
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

    return res->res->Datastore(datastore);
}

/**
 * Subject
 * Gets the subject and length from the current result node, if the result node contains data from a GET
 *
 * @param subject      (optional) the subject of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param subject_len  (optional) the subject_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Subject(void **subject, size_t *subject_len) const {
    if (!Valid()) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *res = Curr();
    if (!res) {
        return HXHIM_ERROR;
    }

    int status = HXHIM_SUCCESS;
    if ((Status(&status) != HXHIM_SUCCESS) ||
        (status != HXHIM_SUCCESS)) {
        return HXHIM_ERROR;
    }

    int rc = HXHIM_ERROR;
    switch (res->type) {
        case hxhim_result_type::HXHIM_RESULT_PUT:
        case hxhim_result_type::HXHIM_RESULT_GET:
        case hxhim_result_type::HXHIM_RESULT_GETOP:
        case hxhim_result_type::HXHIM_RESULT_DEL:
        {
            hxhim::Results::SubjectPredicate *sp = static_cast<hxhim::Results::SubjectPredicate *>(res);
            sp->subject->get(subject, subject_len);
            rc = HXHIM_SUCCESS;
        }
        break;
        default:
            break;
    }

    return rc;
}

/**
 * hxhim_results_subject
 * Gets the subject and length from the current result node, if the result node contains data from a GET
 *
 * @param res          A list of results
 * @param subject      (optional) the subject of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param subject_len  (optional) the subject_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_subject(hxhim_results_t *res, void **subject, size_t *subject_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->Subject(subject, subject_len);
}

/**
 * Predicate
 * Gets the predicate and length from the current result node, if the result node contains data from a GET
 *
 * @param predicate      (optional) the predicate of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param predicate_len  (optional) the predicate_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Predicate(void **predicate, size_t *predicate_len) const {
    if (!Valid()) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *res = Curr();
    if (!res) {
        return HXHIM_ERROR;
    }

    int status = HXHIM_SUCCESS;
    if ((Status(&status) != HXHIM_SUCCESS) ||
        (status != HXHIM_SUCCESS)) {
        return HXHIM_ERROR;
    }

    int rc = HXHIM_ERROR;
    switch (res->type) {
        case hxhim_result_type::HXHIM_RESULT_PUT:
        case hxhim_result_type::HXHIM_RESULT_GET:
        case hxhim_result_type::HXHIM_RESULT_GETOP:
        case hxhim_result_type::HXHIM_RESULT_DEL:
        {
            hxhim::Results::SubjectPredicate *sp = static_cast<hxhim::Results::SubjectPredicate *>(res);
            sp->predicate->get(predicate, predicate_len);
            rc = HXHIM_SUCCESS;
        }
        break;
        default:
            break;
    }

    return rc;
}

/**
 * hxhim_results_predicate
 * Gets the predicate and length from the current result node, if the result node contains data from a GET
 *
 * @param predicate      (optional) the predicate of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param predicate_len  (optional) the predicate_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_predicate(hxhim_results_t *res, void **predicate, size_t *predicate_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->Predicate(predicate, predicate_len);
}

/**
 * ObjectType
 * Gets the object type from the current result node, if the result node contains data from a GET
 *
 * @param object_type (optional) the object type of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::ObjectType(enum hxhim_type_t *object_type) const {
    if (!Valid()) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *res = Curr();
    if (!res) {
        return HXHIM_ERROR;
    }

    int status = HXHIM_SUCCESS;
    if ((Status(&status) != HXHIM_SUCCESS) ||
        (status != HXHIM_SUCCESS)) {
        return HXHIM_ERROR;
    }

    int rc = HXHIM_ERROR;
    switch (res->type) {
        case hxhim_result_type::HXHIM_RESULT_GET:
        case hxhim_result_type::HXHIM_RESULT_GETOP:
        {
            hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(res);
            if (object_type) {
                *object_type = get->object_type;
            }
            rc = HXHIM_SUCCESS;
        }
        break;
        default:
            break;
    }

    return rc;
}

/**
 * hxhim_results_object_type
 * Gets the object type from the current result node, if the result node contains data from a GET
 *
 * @param res         A list of results
 * @param object_type (optional) the object type of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_object_type(hxhim_results_t *res, hxhim_type_t *object_type) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->ObjectType(object_type);
}

/**
 * Object
 * Gets the object and length from the current result node, if the result node contains data from a GET
 *
 * @param object      (optional) the object of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param object_len  (optional) the object_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Object(void **object, size_t *object_len) const {
    if (!Valid()) {
        return HXHIM_ERROR;
    }

    hxhim::Results::Result *res = Curr();
    if (!res) {
        return HXHIM_ERROR;
    }

    int status = HXHIM_SUCCESS;
    if ((Status(&status) != HXHIM_SUCCESS) ||
        (status != HXHIM_SUCCESS)) {
        return HXHIM_ERROR;
    }

    int rc = HXHIM_ERROR;
    switch (res->type) {
        case hxhim_result_type::HXHIM_RESULT_GET:
        case hxhim_result_type::HXHIM_RESULT_GETOP:
        {
            hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(res);
            get->object->get(object, object_len);
            rc = HXHIM_SUCCESS;
        }
        break;
        default:
            break;
    }

    return rc;
}

/**
 * hxhim_results_object
 * Gets the object and length from the current result node, if the result node contains data from a GET
 *
 * @param res         A list of results
 * @param object      (optional) the object of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param object_len  (optional) the object_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_object(hxhim_results_t *res, void **object, size_t *object_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->Object(object, object_len);
}

/**
 * hxhim_results_init
 *
 * @param res A hxhim::Results instance
 * @return the pointer to the C structure containing the hxhim::Results
 */
hxhim_results_t *hxhim_results_init(hxhim_t * hx, hxhim::Results *res) {
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_CLIENT_DBG, "Rank %d Creating hxhim_results_t using %p", rank, res);
    hxhim_results_t *ret = construct<hxhim_results_t>();
    ret->hx = hx;
    ret->res = res;
    mlog(HXHIM_CLIENT_DBG, "Rank %d Created hxhim_results_t %p with %p inside", rank, ret, res);
    return ret;
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
        hxhim::nocheck::GetMPI(res->hx, nullptr, &rank, nullptr);

        mlog(HXHIM_CLIENT_DBG, "Rank %d Destroying hxhim_results_t %p", rank, res);
        hxhim::Results::Destroy(res->res);
        destruct(res);
        mlog(HXHIM_CLIENT_DBG, "Rank %d Destroyed hxhim_results_t %p", rank, res);
    }
}
