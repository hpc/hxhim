#include <cstdlib>
#include <cstring>
#include <sstream>

#include "datastore/datastores.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/private/Results.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/RangeServer.hpp"
#include "utils/Stats.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

hxhim::Results::Result::Timestamps::Timestamps()
    : alloc(nullptr),
      send(),
      transport(),
      recv()
{}

hxhim::Results::Result::Timestamps::~Timestamps() {
    destruct(alloc);
}

hxhim::Results::Result::Result(hxhim_t *hx, const enum hxhim_op_t op,
                               const int range_server, const int ds_status)
    : hx(hx),
      op(op),
      range_server(range_server),
      status((ds_status == DATASTORE_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR),
      timestamps(),
      next(nullptr)
{}

hxhim::Results::Result::~Result() {
    #if PRINT_TIMESTAMPS
    if (hx) {
        const int rank = hx->p->bootstrap.rank;

        const ::Stats::Chronopoint epoch = hx->p->epoch;

        std::stringstream &s = hx->p->print_buffer;

        ::Stats::Chronostamp print;
        print.start = ::Stats::now();

        // from when request was put into the hxhim queue until the response was ready for pulling
        ::Stats::print_event(s, rank, HXHIM_OP_STR[op], epoch, timestamps.send.hash.start,
                                                               timestamps.recv.result.end);
        ::Stats::print_event(s, rank, "Hash",           epoch, timestamps.send.hash);
        ::Stats::print_event(s, rank, "Insert",         epoch, timestamps.send.insert);
        if (timestamps.alloc) {
            ::Stats::print_event(s, rank, "Alloc",      epoch, *timestamps.alloc);
        }
        ::Stats::print_event(s, rank, "ProcessBulk",    epoch, timestamps.transport);

        ::Stats::print_event(s, rank, "Pack",           epoch, timestamps.transport.pack);
        ::Stats::print_event(s, rank, "Transport",      epoch, timestamps.transport.send_start,
                                                               timestamps.transport.recv_end);
        ::Stats::print_event(s, rank, "Unpack",         epoch, timestamps.transport.unpack);
        ::Stats::print_event(s, rank, "Cleanup_RPC",    epoch, timestamps.transport.cleanup_rpc);

        ::Stats::print_event(s, rank, "Result",         epoch, timestamps.recv.result);

        print.end = ::Stats::now();
        ::Stats::print_event(s, rank, "print",          epoch, print);
    }
    #endif
}

hxhim::Results::SubjectPredicate::SubjectPredicate(hxhim_t *hx, const enum hxhim_op_t op,
                                                   const int range_server, const int status)
    : hxhim::Results::Result(hx, op, range_server, status),
      subject(),
      predicate()
{}

hxhim::Results::SubjectPredicate::~SubjectPredicate() {}

hxhim::Results::Put::Put(hxhim_t *hx,
                         const int range_server, const int status)
    : SubjectPredicate(hx, hxhim_op_t::HXHIM_PUT, range_server, status)
{}

hxhim::Results::Delete::Delete(hxhim_t *hx,
                               const int range_server, const int status)
    : SubjectPredicate(hx, hxhim_op_t::HXHIM_DELETE, range_server, status)
{}

hxhim::Results::Sync::Sync(hxhim_t *hx,
                           const int range_server, const int status)
    : Result(hx, hxhim_op_t::HXHIM_SYNC, range_server, status)
{}

hxhim::Results::Hist::Hist(hxhim_t *hx,
                           const int range_server, const int status)
    : Result(hx, hxhim_op_t::HXHIM_HISTOGRAM, range_server, status),
      histogram(nullptr)
{}

hxhim::Results::Result *hxhim::Result::init(hxhim_t *hx, Transport::Response::Response *res, const std::size_t i) {
    // hx should have been checked earlier

    ::Stats::Chronopoint start = ::Stats::now();

    const int rank = hx->p->bootstrap.rank;

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
    if (i == 0) {
        ret->timestamps.alloc = construct<::Stats::Chronostamp>(res->timestamps.allocate);
    }
    ret->timestamps.send = std::move(res->timestamps.reqs[i]);
    ret->timestamps.transport = res->timestamps.transport;
    ret->timestamps.recv.result.start = start;
    ret->timestamps.recv.result.end = ::Stats::now();

    mlog(HXHIM_CLIENT_DBG, "Rank %d Created hxhim::Results::Result %p using %p[%zu]", rank, ret, res, i);

    return ret;
}

hxhim::Results::Put *hxhim::Result::init(hxhim_t *hx, Transport::Response::BPut *bput, const std::size_t i) {
    hxhim::Results::Put *out = construct<hxhim::Results::Put>(hx, bput->src, bput->statuses[i]);

    out->subject = std::move(bput->orig.subjects[i]);
    out->predicate = std::move(bput->orig.predicates[i]);

    return out;
}

hxhim::Results::Get *hxhim::Result::init(hxhim_t *hx, Transport::Response::BGet *bget, const std::size_t i) {
    hxhim::Results::Get *out = construct<hxhim::Results::Get>(hx, bget->src, bget->statuses[i]);

    out->subject = std::move(bget->orig.subjects[i]);
    out->predicate = std::move(bget->orig.predicates[i]);
    if (out->status == HXHIM_SUCCESS) {
        out->object = std::move(bget->objects[i]);
    }
    out->next = nullptr;

    return out;
}

hxhim::Results::GetOp *hxhim::Result::init(hxhim_t *hx, Transport::Response::BGetOp *bgetop, const std::size_t i) {
    const int status = bgetop->statuses[i];

    hxhim::Results::GetOp *top = construct<hxhim::Results::GetOp>(hx, bgetop->src, status);

    hxhim::Results::GetOp *prev = nullptr;
    hxhim::Results::GetOp *curr = top;
    for(std::size_t j = 0; j < bgetop->num_recs[i]; j++) {
        curr->subject = std::move(bgetop->subjects[i][j]);
        curr->predicate = std::move(bgetop->predicates[i][j]);

        if (curr->status == HXHIM_SUCCESS) {
            curr->object = std::move(bgetop->objects[i][j]);
        }

        prev = curr;
        curr = construct<hxhim::Results::GetOp>(hx, bgetop->src, status);
        prev->next = curr;
    }

    // drop last item (empty)
    prev->next = nullptr;
    destruct(curr);

    return top;
}

hxhim::Results::Delete *hxhim::Result::init(hxhim_t *hx, Transport::Response::BDelete *bdel, const std::size_t i) {
    hxhim::Results::Delete *out = construct<hxhim::Results::Delete>(hx, bdel->src, bdel->statuses[i]);

    out->subject = std::move(bdel->orig.subjects[i]);
    out->predicate = std::move(bdel->orig.predicates[i]);

    return out;
}

hxhim::Results::Sync *hxhim::Result::init(hxhim_t *hx, const int synced) {
    const int rank = hx->p->bootstrap.rank;
    return construct<hxhim::Results::Sync>(hx, rank, synced);
}

hxhim::Results::Hist *hxhim::Result::init(hxhim_t *hx, Transport::Response::BHistogram *bhist, const std::size_t i) {
    hxhim::Results::Hist *out = construct<hxhim::Results::Hist>(hx, bhist->src, bhist->statuses[i]);

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
uint64_t hxhim::Result::AddAll(hxhim_t *hx, hxhim::Results *results,
                               Transport::Response::Response *response) {
    ::Stats::Chronopoint start = ::Stats::now();

    uint64_t duration = 0;
    for(Transport::Response::Response *res = response; res; res = next(res)) {
        duration += ::Stats::nano(res->timestamps.transport.start,
                                  res->timestamps.transport.end);

        for(std::size_t i = 0; i < res->count; i++) {
            hxhim::Results::Result *result = results->Add(hxhim::Result::init(hx, res, i));

            // add timestamps of individual results
            duration += ::Stats::nano(result->timestamps.send.hash.start,
                                      result->timestamps.send.insert.end) +
                        ::Stats::nano(result->timestamps.recv.result.start,
                                      result->timestamps.recv.result.end);
        }
    }

    ::Stats::Chronopoint end = ::Stats::now();

    duration += ::Stats::nano(start, end);
    results->UpdateDuration(duration);

    return duration;
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

hxhim::Results::Results()
    : head(nullptr),
      tail(nullptr),
      curr(nullptr),
      count(0),
      duration(0)
{}

hxhim::Results::~Results() {
    for(curr = head; curr;) {
        Result *next = curr->next;
        destruct(curr);
        curr = next;
    }

    head = nullptr;
    tail = nullptr;
    count = 0;
}

/**
 * Destroy
 * Destroys the results returned from an operation
 *
 * @param hx    the HXHIM session
 * @param res   pointer to a set of results
 */
void hxhim::Results::Destroy(Results *res) {
    destruct(res);
}

/**
 * push_back
 * The actual list push_back operation
 * count is incremented. response->next is not reset
 * to allow for another list to be appended to the tail
 *
 * @param response the result packet to put at the back
 */
void hxhim::Results::push_back(hxhim::Results::Result *response) {
    // don't push nullptrs
    if (!response) {
        return;
    }

    if (!head) {
        head = response;
    }

    if (tail) {
        tail->next = response; // do not reset response->next
        tail = tail->next;
    }
    else {
        tail = response;
    }

    // curr is not modified

    count++;
}

/**
 * Add
 * Appends a single result node to the end of the list
 * The result type is processed before being inserted
 *
 * @return the pointer to the last valid node
 */
hxhim::Results::Result *hxhim::Results::Add(hxhim::Results::Result *response) {
    if (response) {
        // serialize GetOps
        if (response->op == hxhim_op_t::HXHIM_GETOP) {
            for(hxhim::Results::GetOp *get = static_cast<hxhim::Results::GetOp *>(response);
                get; get = get->next) {
                push_back(get);
            }
        }
        else {
            push_back(response);
        }
    }

    return tail;
}

/**
 * UpdateDuration
 * Change the time spent to collect this set of results.
 * Needed since individual results do not have the packet
 * time, and adding the packet time with each result is
 * incorrect. Also needed because Transport::Message
 * should not be exposed.
 *
 * @param  ns   the change in nanoseconds
 * @return the duration before the change in nanoseconds
 */
uint64_t hxhim::Results::UpdateDuration(const uint64_t ns) {
    const uint64_t old_duration = duration;
    duration += ns;
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
        if (other->count) {
            push_back(other->head);    // move other into this instance
            tail = other->tail;        // override tail since the actual tail is other->tail
            count += other->count - 1; // subtract 1 because count was incremented in push_back
            duration += other->duration;
        }

        // clear out other
        other->head = nullptr;
        other->tail = nullptr;
        other->curr = nullptr;
        other->count = 0;
        other->duration = 0;
    }
}

/**
 * ValidIterator
 *
 * @return Whether or not the iterator is at a valid position
 */
bool hxhim::Results::ValidIterator() const {
    return curr;
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
    return (curr = head);
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
        curr = curr->next;
    }

    return curr;
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
    return curr;
}

/**
 * Size
 * Get the number of elements in this set of results
 *
 * @return number of elements
 */
std::size_t hxhim::Results::Size() const {
    return count;
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
 * @return the total time in nanoseconds
 */
uint64_t hxhim::Results::Duration() const {
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
int hxhim_results_duration(hxhim_results_t *res, uint64_t *duration) {
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
 * RangeServer
 * Gets the range server of the result node currently being pointed to
 *
 * @param range_server  (optional) the range_server of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::RangeServer(int *range_server) const {
    hxhim::Results::Result *res = Curr();
    if (!res) {
        return HXHIM_ERROR;
    }

    if (range_server) {
        *range_server = res->range_server;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_result_range_server
 * Gets the range_server of the result node currently being pointed to
 *
 * @param res           A list of results
 * @param range_server  (optional) the range_server of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_range_server(hxhim_results_t *res, int *range_server) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->RangeServer(range_server);
}

/**
 * Subject
 * Gets the subject and length from the current result node, if the result node contains data from a GET
 *
 * @param subject      (optional) the subject of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param subject_len  (optional) the subject_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param subject_type (optional) the type of the subject
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Subject(void **subject, std::size_t *subject_len, enum hxhim_data_t *subject_type) const {
    hxhim::Results::Result *res = Curr();
    if (!res                                       ||
        ((res->op    != hxhim_op_t::HXHIM_PUT)     &&
         (res->op    != hxhim_op_t::HXHIM_GET)     &&
         (res->op    != hxhim_op_t::HXHIM_GETOP)   &&
         (res->op    != hxhim_op_t::HXHIM_DELETE))) {
        return HXHIM_ERROR;
    }

    hxhim::Results::SubjectPredicate *sp = static_cast<hxhim::Results::SubjectPredicate *>(res);
    sp->subject.get(subject, subject_len, subject_type);

    return HXHIM_SUCCESS;
}

/**
 * hxhim_result_subject
 * Gets the subject and length from the current result node, if the result node contains data from a GET
 *
 * @param res          A list of results
 * @param subject      (optional) the subject of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param subject_len  (optional) the subject_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param subject_type (optional) the type of the subject
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_subject(hxhim_results_t *res, void **subject, size_t *subject_len, hxhim_data_t *subject_type) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->Subject(subject, subject_len, subject_type);
}

/**
 * Predicate
 * Gets the predicate and length from the current result node, if the result node contains data from a GET
 *
 * @param predicate      (optional) the predicate of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param predicate_len  (optional) the predicate_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param predicate_type (optional) the type of the predicate
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Predicate(void **predicate, std::size_t *predicate_len, hxhim_data_t *predicate_type) const {
    hxhim::Results::Result *res = Curr();
    if (!res                                       ||
        ((res->op    != hxhim_op_t::HXHIM_PUT)     &&
         (res->op    != hxhim_op_t::HXHIM_GET)     &&
         (res->op    != hxhim_op_t::HXHIM_GETOP)   &&
         (res->op    != hxhim_op_t::HXHIM_DELETE))) {
        return HXHIM_ERROR;
    }

    hxhim::Results::SubjectPredicate *sp = static_cast<hxhim::Results::SubjectPredicate *>(res);
    sp->predicate.get(predicate, predicate_len, predicate_type);
    return HXHIM_SUCCESS;
}

/**
 * hxhim_result_predicate
 * Gets the predicate and length from the current result node, if the result node contains data from a GET
 *
 * @param predicate      (optional) the predicate of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param predicate_len  (optional) the predicate_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param predicate_type (optional) the type of the predicate
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_predicate(hxhim_results_t *res, void **predicate, size_t *predicate_len, enum hxhim_data_t *predicate_type) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->Predicate(predicate, predicate_len, predicate_type);
}

/**
 * Object
 * Gets the object and length from the current result node, if the result node contains data from a GET
 *
 * @param object      (optional) the object of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param object_len  (optional) the object_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param object_type (optional) the type of the object
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Object(void **object, std::size_t *object_len, hxhim_data_t *object_type) const {
    hxhim::Results::Result *res = Curr();
    if (!res                                      ||
        ((res->op    != hxhim_op_t::HXHIM_GET)    &&
         (res->op    != hxhim_op_t::HXHIM_GETOP)) ||
        (res->status != HXHIM_SUCCESS))            {
        return HXHIM_ERROR;
    }

    hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(res);
    get->object.get(object, object_len, object_type);
    return HXHIM_SUCCESS;
}

/**
 * hxhim_result_object
 * Gets the object and length from the current result node, if the result node contains data from a GET
 *
 * @param res         A list of results
 * @param object      (optional) the object of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param object_len  (optional) the object_len of the current result, only valid if this function returns HXHIM_SUCCESS
 * @param object_type (optional) the type of the object
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_object(hxhim_results_t *res, void **object, size_t *object_len, enum hxhim_data_t *object_type) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->Object(object, object_len, object_type);
}

/**
 * Histogram
 * Gets the histogram data from the current result node, if the result node contains data from a HISTOGRAM
 *
 * @param name      (optional) the name of the histogram
 * @param name_len  (optional) the length of the histogram's name
 * @param buckets   (optional) the buckets of the histogram
 * @param counts    (optional) the counts of the histogram
 * @param size      (optional) how many bucket-count pairs there are
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Histogram(const char **name, std::size_t *name_len, double **buckets, std::size_t **counts, std::size_t *size) const {
    hxhim::Results::Result *res = Curr();
    if (!res                                         ||
        (res->op     != hxhim_op_t::HXHIM_HISTOGRAM) ||
        (res->status != HXHIM_SUCCESS))               {
        return HXHIM_ERROR;
    }

    hxhim::Results::Hist *hist = static_cast<hxhim::Results::Hist *>(res);
    if (hist->histogram->get_name(name, name_len) != HISTOGRAM_SUCCESS) {
        return HXHIM_ERROR;
    }

    if (hist->histogram->get(buckets, counts, size) != HISTOGRAM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_result_histogram
 * Gets the histogram and length from the current result node, if the result node contains data from a GET
 *
 * @param res       A list of results
 * @param name      (optional) the name of the histogram
 * @param name_len  (optional) the length of the histogram's name
 * @param buckets   (optional) the buckets of the histogram
 * @param counts    (optional) the counts of the histogram
 * @param size      (optional) how many bucket-count pairs there are
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim_result_histogram(hxhim_results_t *res, const char **name, std::size_t *name_len, double **buckets, size_t **counts, size_t *size) {
    if (hxhim_results_valid(res) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return res->res->Histogram(name, name_len, buckets, counts, size);
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
    const int rank = hx->p->bootstrap.rank;

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
        const int rank = res->hx->p->bootstrap.rank;

        mlog(HXHIM_CLIENT_DBG, "Rank %d Destroying hxhim_results_t %p", rank, res);
        hxhim::Results::Destroy(res->res);
        destruct(res);
        mlog(HXHIM_CLIENT_DBG, "Rank %d Destroyed hxhim_results_t %p", rank, res);
    }
}
