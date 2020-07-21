#include <cstdlib>
#include <cstring>
#include <sstream>

#include "datastore/datastores.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/private/Results.hpp"
#include "hxhim/private/accessors.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

const char *hxhim_result_type_str[] = {
    "NONE",
    "PUT",
    "GET",
    "GETOP",
    "DELETE",
    "SYNC",
    "MAX",
};

hxhim::Results::Result::Result(hxhim_t *hx, const hxhim_result_type_t type,
                               const int datastore, const int status)
    : hx(hx),
      type(type),
      datastore(datastore),
      status(status)
{}

hxhim::Results::Result::~Result() {
    if (hx) {
        struct timespec epoch;
        hxhim::nocheck::GetEpoch(hx, &epoch);
        std::stringstream s;
        s << hxhim_result_type_str[type]                                                      << std::endl
          << "    Cached:           " << elapsed2(&epoch, &timestamps.send.cached)            << std::endl
          << "    Shuffled:         " << elapsed2(&epoch, &timestamps.send.shuffled)          << std::endl
          << "    Hash Start:       " << elapsed2(&epoch, &timestamps.send.hashed.start)      << std::endl
          << "    Hash End:         " << elapsed2(&epoch, &timestamps.send.hashed.end)        << std::endl
          << "    Find Dst (total): " << timestamps.send.find_dst                             << std::endl
          << "    Bulked Start:     " << elapsed2(&epoch, &timestamps.send.bulked.start)      << std::endl
          << "    Bulked End:       " << elapsed2(&epoch, &timestamps.send.bulked.end)        << std::endl
          << "    Pack Start:       " << elapsed2(&epoch, &timestamps.transport.pack.start)   << std::endl
          << "    Pack End:         " << elapsed2(&epoch, &timestamps.transport.pack.end)     << std::endl
          << "    Send Start:       " << elapsed2(&epoch, &timestamps.transport.send_start)   << std::endl
          << "    Recv End:         " << elapsed2(&epoch, &timestamps.transport.recv_end)     << std::endl
          << "    Unpack Start:     " << elapsed2(&epoch, &timestamps.transport.unpack.start) << std::endl
          << "    Unpack End:       " << elapsed2(&epoch, &timestamps.transport.unpack.end)   << std::endl
          << "    Result Start:     " << elapsed2(&epoch, &timestamps.recv.result.start)      << std::endl
          << "    Result End:       " << elapsed2(&epoch, &timestamps.recv.result.end)        << std::endl;

        mlog(HXHIM_CLIENT_DBG, "\n%s", s.str().c_str());
    }
}

hxhim::Results::SubjectPredicate::SubjectPredicate(hxhim_t *hx, const hxhim_result_type_t type,
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
    : SubjectPredicate(hx, hxhim_result_type_t::HXHIM_RESULT_PUT, datastore, status)
{}

hxhim::Results::Delete::Delete(hxhim_t *hx,
                               const int datastore, const int status)
    : SubjectPredicate(hx, hxhim_result_type_t::HXHIM_RESULT_DEL, datastore, status)
{}

hxhim::Results::Sync::Sync(hxhim_t *hx,
                           const int datastore, const int status)
    : Result(hx, hxhim_result_type_t::HXHIM_RESULT_SYNC, datastore, status)
{}

hxhim::Results::Result *hxhim::Result::init(hxhim_t *hx, Transport::Response::Response *res, const std::size_t i) {
    // hx should have been checked earlier

    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);

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

    // set timestamps
    ret->timestamps.send = res->timestamps.reqs[i];
    ret->timestamps.transport = res->timestamps.transport;
    ret->timestamps.recv.result.start = start;
    clock_gettime(CLOCK_MONOTONIC, &ret->timestamps.recv.result.end);

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
    for(std::size_t j = 0; j < bgetop->num_recs[i]; j++) {
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

/**
 * hxhim_results_valid
 *
 * @param res A list of results
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_valid(hxhim_results_t *res) {
    return (res && res->res)?HXHIM_SUCCESS:HXHIM_ERROR;
}

hxhim::Results::Results(hxhim_t *hx)
    : hx(hx),
      results(),
      curr(results.end()),
      duration(0)
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
 * Add
 * Appends a single result node to the end of the list
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
 * Add
 * Converts an entire response packet into a result list
 * Timestamps are summed up
 *
 * @param res the response packet
 */
void hxhim::Results::Add(Transport::Response::Response *res) {
    for(Transport::Response::Response *curr = res; curr; curr = next(curr)) {
        struct timespec start;
        clock_gettime(CLOCK_MONOTONIC, &start);
        for(std::size_t i = 0; i < curr->count; i++) {
            Add(hxhim::Result::init(hx, curr, i));

            duration += elapsed(&curr->timestamps.reqs[i].hashed)
                     +  curr->timestamps.reqs[i].find_dst
                     +  elapsed(&curr->timestamps.reqs[i].bulked);
        }

        duration += elapsed2(&curr->timestamps.transport.pack.start,
                             &curr->timestamps.transport.unpack.end);

        struct timespec end;
        clock_gettime(CLOCK_MONOTONIC, &end);

        duration += elapsed2(&start, &end);
    }
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

        duration += other->duration;
        other->duration = 0;
    }
}

/**
 * ValidIterator
 *
 * @return Whether or not the iterator is at a valid position
 */
bool hxhim::Results::ValidIterator() const {
    return (curr != results.end());
}

/**
 * hxhim_results_valid_iterator
 *
 * @param res A list of results
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_valid_iterator(hxhim_results_t *res) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->ValidIterator()?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * GoToHead
 * Moves the current node to point to the head of the list
 *
 * @return Whether or not the new position is valid
 */
hxhim::Results::Result *hxhim::Results::GoToHead() {
    curr = results.begin();
    return ValidIterator()?*curr:nullptr;
}

/**
 * hxhim_results_goto_head
 * Moves the internal pointer to the head of the list
 *
 * @param res A list of results
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_goto_head(hxhim_results_t *res) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->GoToHead()?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * GoToNext
 * Moves the current node to point to the next node in the list
 *
 * @return Whether or not the new position is valid
 */
hxhim::Results::Result *hxhim::Results::GoToNext() {
    curr++;
    return ValidIterator()?*curr:nullptr;
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
    return ValidIterator()?*curr:nullptr;;
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
 * hxhim_results_size
 * Get the number of elements in this set of results
 *
 * @param res   A list of results
 * @param size  The number of elements
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_size(hxhim_results_t *res, size_t *size) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    if (size) {
        *size = res->res->Size();
    }

    return HXHIM_SUCCESS;
}

/**
 * Duration
 * Return the total time it took to convert
 * requests to responses.
 *
 * @return the total time
 */
long double hxhim::Results::Duration() const {
    return duration;
}

/**
 * Duration
 * Return the total time it took to convert
 * requests to responses.
 *
 * @param res       A list of results
 * @param duration  The total time
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_results_duration(hxhim_results_t *res, long double *duration) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    if (duration) {
        *duration = res->res->Duration();
    }

    return HXHIM_SUCCESS;
}

/**
 * Type
 * Gets the type of the result node currently being pointed to
 *
 * @param type  (optional) the type of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Type(enum hxhim_result_type_t *type) const {
    hxhim::Results::Result *res = Curr();
    if (!res) {
        return HXHIM_ERROR;
    }

    if (type) {
        *type = res->type;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_result_type
 * Gets the type of the result node currently being pointed to
 *
 * @param res   A list of results
 * @param type  (optional) the type of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_type(hxhim_results_t *res, enum hxhim_result_type_t *type) {
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
    hxhim::Results::Result *res = Curr();
    if (!res) {
        return HXHIM_ERROR;
    }

    if (status) {
        *status = res->status;
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
int hxhim_result_status(hxhim_results_t *res, int *status) {
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
    hxhim::Results::Result *res = Curr();
    if (!res) {
        return HXHIM_ERROR;
    }

    if (datastore) {
        *datastore = res->datastore;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_result_datastore
 * Gets the datastore of the result node currently being pointed to
 *
 * @param res       A list of results
 * @param datastore  (optional) the datastore of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_datastore(hxhim_results_t *res, int *datastore) {
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
        case hxhim_result_type_t::HXHIM_RESULT_PUT:
        case hxhim_result_type_t::HXHIM_RESULT_GET:
        case hxhim_result_type_t::HXHIM_RESULT_GETOP:
        case hxhim_result_type_t::HXHIM_RESULT_DEL:
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
 * hxhim_result_subject
 * Gets the subject and length from the current result node, if the result node contains data from a GET
 *
 * @param res          A list of results
 * @param subject      (optional) the subject of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param subject_len  (optional) the subject_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_subject(hxhim_results_t *res, void **subject, size_t *subject_len) {
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
        case hxhim_result_type_t::HXHIM_RESULT_PUT:
        case hxhim_result_type_t::HXHIM_RESULT_GET:
        case hxhim_result_type_t::HXHIM_RESULT_GETOP:
        case hxhim_result_type_t::HXHIM_RESULT_DEL:
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
 * hxhim_result_predicate
 * Gets the predicate and length from the current result node, if the result node contains data from a GET
 *
 * @param predicate      (optional) the predicate of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param predicate_len  (optional) the predicate_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_predicate(hxhim_results_t *res, void **predicate, size_t *predicate_len) {
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
        case hxhim_result_type_t::HXHIM_RESULT_GET:
        case hxhim_result_type_t::HXHIM_RESULT_GETOP:
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
 * hxhim_result_object_type
 * Gets the object type from the current result node, if the result node contains data from a GET
 *
 * @param res         A list of results
 * @param object_type (optional) the object type of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_object_type(hxhim_results_t *res, hxhim_type_t *object_type) {
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
        case hxhim_result_type_t::HXHIM_RESULT_GET:
        case hxhim_result_type_t::HXHIM_RESULT_GETOP:
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
 * hxhim_result_object
 * Gets the object and length from the current result node, if the result node contains data from a GET
 *
 * @param res         A list of results
 * @param object      (optional) the object of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param object_len  (optional) the object_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_object(hxhim_results_t *res, void **object, size_t *object_len) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->Object(object, object_len);
}

/**
 * Timestamps
 * Extract the timestamps associated with this request
 *
 * @param timestamps (optional) the address of the pointer to fill in
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Timestamps(struct hxhim_result_timestamps_t **timestamps) const {
    hxhim::Results::Result *res = Curr();
    if (!res) {
        return HXHIM_ERROR;
    }

    if (timestamps) {
        *timestamps = &res->timestamps;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_result_timestamps
 * Extract the timestamps associated with this request
 *
 * @param res         A list of results
 * @param timestamps (optional) the address of the pointer to fill in
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_timestamps(hxhim_results_t *res, struct hxhim_result_timestamps_t **timestamps) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->Timestamps(timestamps);
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
