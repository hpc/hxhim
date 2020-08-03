#include <cstdlib>
#include <cstring>
#include <sstream>

#include "datastore/datastores.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/private/Results.hpp"
#include "hxhim/private/accessors.hpp"
#include "utils/Stats.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

hxhim::Results::Result::Timestamps::Timestamps()
    : send(nullptr),
      transport(),
      recv()
{}

hxhim::Results::Result::Result(hxhim_t *hx, const enum hxhim_op_t op,
                               const int datastore, const int status)
    : hx(hx),
      op(op),
      datastore(datastore),
      status(status),
      timestamps()
{}

hxhim::Results::Result::~Result() {
    if (hx) {
        int rank = -1;
        hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

        ::Stats::Chronopoint epoch;
        hxhim::nocheck::GetEpoch(hx, epoch);

        std::stringstream s;

        // from when request was put into the hxhim queue until the response was ready for pulling
        ::Stats::print_event(s, rank, HXHIM_OP_STR[op], epoch, timestamps.send->cached,
                                                               timestamps.recv.result.end);
        ::Stats::print_event(s, rank, "Cached",         epoch, timestamps.send->cached);
        ::Stats::print_event(s, rank, "Shuffled",       epoch, timestamps.send->shuffled);
        ::Stats::print_event(s, rank, "Hash",           epoch, timestamps.send->hashed);
        // This might take very long
        for(::Stats::Chronostamp const find : timestamps.send->find_dsts) {
            ::Stats::print_event(s, rank, "FindDst",    epoch, find);
        }
        ::Stats::print_event(s, rank, "Bulked",         epoch, timestamps.send->bulked);
        ::Stats::print_event(s, rank, "ProcessBulk",    epoch, timestamps.transport.start,
                                                               timestamps.transport.end);

        ::Stats::print_event(s, rank, "Pack",           epoch, timestamps.transport.pack);
        ::Stats::print_event(s, rank, "Transport",      epoch, timestamps.transport.send_start,
                                                               timestamps.transport.recv_end);
        ::Stats::print_event(s, rank, "Unpack",         epoch, timestamps.transport.unpack);
        ::Stats::print_event(s, rank, "Result",         epoch, timestamps.recv.result);

        mlog(HXHIM_CLIENT_NOTE, "\n%s", s.str().c_str());
    }

    destruct(timestamps.send);
}

hxhim::Results::SubjectPredicate::SubjectPredicate(hxhim_t *hx, const enum hxhim_op_t op,
                                                   const int datastore, const int status)
    : hxhim::Results::Result(hx, op, datastore, status),
      subject(),
      predicate()
{}

hxhim::Results::SubjectPredicate::~SubjectPredicate() {}

hxhim::Results::Put::Put(hxhim_t *hx,
                         const int datastore, const int status)
    : SubjectPredicate(hx, hxhim_op_t::HXHIM_PUT, datastore, status)
{}

hxhim::Results::Delete::Delete(hxhim_t *hx,
                               const int datastore, const int status)
    : SubjectPredicate(hx, hxhim_op_t::HXHIM_DELETE, datastore, status)
{}

hxhim::Results::Sync::Sync(hxhim_t *hx,
                           const int datastore, const int status)
    : Result(hx, hxhim_op_t::HXHIM_SYNC, datastore, status)
{}

hxhim::Results::Hist::Hist(hxhim_t *hx,
                           const int datastore, const int status)
    : Result(hx, hxhim_op_t::HXHIM_HISTOGRAM, datastore, status),
      histogram(nullptr)
{}

hxhim::Results::Result *hxhim::Result::init(hxhim_t *hx, Transport::Response::Response *res, const std::size_t i) {
    // hx should have been checked earlier

    ::Stats::Chronopoint start = ::Stats::now();

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
    switch (res->op) {
        case hxhim_op_t::HXHIM_PUT:
            ret = init(hx, static_cast<Transport::Response::BPut *>(res), i);
            break;
        case hxhim_op_t::HXHIM_GET:
            ret = init(hx, static_cast<Transport::Response::BGet *>(res), i);
            break;
        case hxhim_op_t::HXHIM_GETOP:
            ret = init(hx, static_cast<Transport::Response::BGetOp *>(res), i);
            break;
        case hxhim_op_t::HXHIM_DELETE:
            ret = init(hx, static_cast<Transport::Response::BDelete *>(res), i);
            break;
        case hxhim_op_t::HXHIM_HISTOGRAM:
            ret = init(hx, static_cast<Transport::Response::BHistogram *>(res), i);
            break;
        default:
            break;
    }

    // set timestamps
    ret->timestamps.send = res->timestamps.reqs[i];
    ret->timestamps.transport = res->timestamps.transport;
    ret->timestamps.recv.result.start = start;
    ret->timestamps.recv.result.end = ::Stats::now();
    res->timestamps.reqs[i] = nullptr;

    mlog(HXHIM_CLIENT_DBG, "Rank %d Created hxhim::Results::Result %p using %p[%zu]", rank, ret, res, i);

    return ret;
}

hxhim::Results::Put *hxhim::Result::init(hxhim_t *hx, Transport::Response::BPut *bput, const std::size_t i) {
    hxhim::Results::Put *out = construct<hxhim::Results::Put>(hx, hxhim::datastore::get_id(hx, bput->src, bput->ds_offsets[i]), bput->statuses[i]);

    out->subject = bput->orig.subjects[i];
    out->predicate = bput->orig.predicates[i];

    return out;
}

hxhim::Results::Get *hxhim::Result::init(hxhim_t *hx, Transport::Response::BGet *bget, const std::size_t i) {
    hxhim::Results::Get *out = construct<hxhim::Results::Get>(hx, hxhim::datastore::get_id(hx, bget->src, bget->ds_offsets[i]), bget->statuses[i]);

    out->subject = bget->orig.subjects[i];
    out->predicate = bget->orig.predicates[i];
    out->object_type = bget->object_types[i];
    out->object = bget->objects[i];
    out->next = nullptr;

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

    return out;
}

hxhim::Results::Sync *hxhim::Result::init(hxhim_t *hx, const int ds_offset, const int synced) {
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    hxhim::Results::Sync *out = construct<hxhim::Results::Sync>(hx, hxhim::datastore::get_id(hx, rank, ds_offset), synced);
    return out;
}

hxhim::Results::Hist *hxhim::Result::init(hxhim_t *hx, Transport::Response::BHistogram *bhist, const std::size_t i) {
    hxhim::Results::Hist *out = construct<hxhim::Results::Hist>(hx, hxhim::datastore::get_id(hx, bhist->src, bhist->ds_offsets[i]), bhist->statuses[i]);

    out->histogram = bhist->histograms[i];

    bhist->histograms[i] = nullptr;
    return out;
}

/**
 * AddAll
 * Converts an entire response packet into a result list
 * Timestamps are summed up and updated in the target
 * results list
 *
 * @param results   the result list to insert into
 * @param response  the response packet
 */
void hxhim::Result::AddAll(hxhim_t *hx, hxhim::Results *results, Transport::Response::Response *response) {
    long double duration = 0;
    for(Transport::Response::Response *res = response; res; res = next(res)) {
        ::Stats::Chronopoint start = ::Stats::now();

        duration += ::Stats::sec(res->timestamps.transport.pack.start,
                                 res->timestamps.transport.unpack.end);

        for(std::size_t i = 0; i < res->count; i++) {
            for(::Stats::Chronostamp const &find_dst : res->timestamps.reqs[i]->find_dsts) {
                duration += ::Stats::sec(find_dst);
            }

            duration += ::Stats::sec(res->timestamps.reqs[i]->hashed)
                     +  ::Stats::sec(res->timestamps.reqs[i]->bulked);

            results->Add(hxhim::Result::init(hx, res, i));
        }

        duration += ::Stats::sec(start, ::Stats::now());
    }

    results->UpdateDuration(duration);
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
hxhim::Results::Result *hxhim::Results::Add(hxhim::Results::Result *response) {
    if (response) {
        // serialize GetOps
        if (response->op == hxhim_op_t::HXHIM_GETOP) {
            for(hxhim::Results::GetOp *get = static_cast<hxhim::Results::GetOp *>(response);
                get; get = get->next) {
                results.push_back(get);
            }
        }
        else {
            results.push_back(response);
        }

        // explicitly invalidate iterator
        curr = results.end();
    }
    decltype(results)::reverse_iterator it = results.rbegin();
    return (it != results.rend())?*it:nullptr;
}

/**
 * UpdateDuration
 * Change the time spent to collect this set of results.
 * Needed since individual results do not have the packet
 * time, and adding the packet time with each result is
 * incorrect. Also needed because Transport::Message
 * should not be exposed.
 *
 * @param  dt   the change in duration
 * @return the duration before adding dt
 */
long double hxhim::Results::UpdateDuration(const long double dt) {
    const long double old_duration = duration;
    duration += dt;
    return old_duration;
}

/**
 * Append
 * Moves and appends the contents of another hxhim::Results into this one.
 * The other list is emptied out
 *
 * @return the pointer to the construct<node
 */
void hxhim::Results::Append(hxhim::Results *other) {
    if (other) {
        results.insert(results.end(), other->results.begin(), other->results.end());
        duration += other->duration;

        other->results.clear();
        other->duration = 0;

        // explicitly invalidate iterator
        curr = results.end();
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
    if (ValidIterator()) {
        curr++;
    }

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
 * Op
 * Get the operation that was performed to get the current result
 *
 * @param type  (optional) the type of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Op(enum hxhim_op_t *op) const {
    hxhim::Results::Result *res = Curr();
    if (!res) {
        return HXHIM_ERROR;
    }

    if (op) {
        *op = res->op;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_result_op
 * Get the operation that was performed to get the current result
 *
 * @param res   A list of results
 * @param op  (optional) the op of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_op(hxhim_results_t *res, enum hxhim_op_t *op) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->Op(op);
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
int hxhim::Results::Subject(void **subject, std::size_t *subject_len) const {
    hxhim::Results::Result *res = Curr();
    if (!res                                       ||
        ((res->op    != hxhim_op_t::HXHIM_PUT)     &&
         (res->op    != hxhim_op_t::HXHIM_GET)     &&
         (res->op    != hxhim_op_t::HXHIM_GETOP)   &&
         (res->op    != hxhim_op_t::HXHIM_DELETE)) ||
        (res->status != HXHIM_SUCCESS))             {
        return HXHIM_ERROR;
    }

    hxhim::Results::SubjectPredicate *sp = static_cast<hxhim::Results::SubjectPredicate *>(res);
    sp->subject.get(subject, subject_len);
    return HXHIM_SUCCESS;
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
int hxhim::Results::Predicate(void **predicate, std::size_t *predicate_len) const {
    hxhim::Results::Result *res = Curr();
    if (!res                                       ||
        ((res->op    != hxhim_op_t::HXHIM_PUT)     &&
         (res->op    != hxhim_op_t::HXHIM_GET)     &&
         (res->op    != hxhim_op_t::HXHIM_GETOP)   &&
         (res->op    != hxhim_op_t::HXHIM_DELETE)) ||
        (res->status != HXHIM_SUCCESS))             {
        return HXHIM_ERROR;
    }

    hxhim::Results::SubjectPredicate *sp = static_cast<hxhim::Results::SubjectPredicate *>(res);
    sp->predicate.get(predicate, predicate_len);
    return HXHIM_SUCCESS;
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
int hxhim::Results::ObjectType(enum hxhim_object_type_t *object_type) const {
    hxhim::Results::Result *res = Curr();
    if (!res                                       ||
        ((res->op    != hxhim_op_t::HXHIM_GET)     &&
         (res->op    != hxhim_op_t::HXHIM_GETOP))  ||
        (res->status != HXHIM_SUCCESS))             {
        return HXHIM_ERROR;
    }

    hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(res);
    if (object_type) {
        *object_type = get->object_type;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_result_object_type
 * Gets the object type from the current result node, if the result node contains data from a GET
 *
 * @param res         A list of results
 * @param object_type (optional) the object type of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_object_type(hxhim_results_t *res, enum hxhim_object_type_t *object_type) {
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
int hxhim::Results::Object(void **object, std::size_t *object_len) const {
    hxhim::Results::Result *res = Curr();
    if (!res                                      ||
        ((res->op    != hxhim_op_t::HXHIM_GET)    &&
         (res->op    != hxhim_op_t::HXHIM_GETOP)) ||
        (res->status != HXHIM_SUCCESS))            {
        return HXHIM_ERROR;
    }

    hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(res);
    get->object.get(object, object_len);
    return HXHIM_SUCCESS;
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
 * Histogram
 * Gets the histogram data from the current result node, if the result node contains data from a HISTOGRAM
 *
 * @param buckets   (optional) the buckets of the histogram
 * @param counts    (optional) the counts of the histogram
 * @param size      (optional) how many bucket-count pairs there are
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Histogram(double **buckets, std::size_t **counts, std::size_t *size) const {
    hxhim::Results::Result *res = Curr();
    if (!res                                         ||
        (res->op     != hxhim_op_t::HXHIM_HISTOGRAM) ||
        (res->status != HXHIM_SUCCESS))               {
        return HXHIM_ERROR;
    }

    hxhim::Results::Hist *hist = static_cast<hxhim::Results::Hist *>(res);
    return (hist->histogram->get(buckets, counts, size) == HISTOGRAM_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * hxhim_result_histogram
 * Gets the histogram and length from the current result node, if the result node contains data from a GET
 *
 * @param res       A list of results
 * @param buckets   (optional) the buckets of the histogram
 * @param counts    (optional) the counts of the histogram
 * @param size      (optional) how many bucket-count pairs there are
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_histogram(hxhim_results_t *res, double **buckets, size_t **counts, size_t *size) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->Histogram(buckets, counts, size);
}

/**
 * Histogram
 * Gets the histogram data from the current result node, if the result node contains data from a HISTOGRAM
 *
 * @param hist      (optional) the histogram of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Histogram(::Histogram::Histogram **hist) const {
    hxhim::Results::Result *res = Curr();
    if (!res                                         ||
        (res->op     != hxhim_op_t::HXHIM_HISTOGRAM) ||
        (res->status != HXHIM_SUCCESS))               {
        return HXHIM_ERROR;
    }

    if (hist) {
        hxhim::Results::Hist *h = static_cast<hxhim::Results::Hist *>(res);
        *hist = h->histogram.get();
    }

    return HXHIM_SUCCESS;
}

/**
 * Timestamps
 * Extract the timestamps associated with this request
 *
 * @param timestamps (optional) the address of the pointer to fill in
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Timestamps(struct hxhim::Results::Result::Timestamps **timestamps) const {
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
