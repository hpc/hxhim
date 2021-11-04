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

hxhim::Result::Timestamps::Timestamps()
    : alloc(nullptr),
      send(),
      transport(),
      recv()
{}

hxhim::Result::Timestamps::~Timestamps() {
    destruct(alloc);
}

hxhim::Result::Result::Result(hxhim_t *hx, const enum hxhim_op_t op,
                              const int range_server, const int ds_status)
    : hx(hx),
      op(op),
      range_server(range_server),
      status((ds_status == DATASTORE_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR),
      timestamps()
{}

hxhim::Result::Result::~Result() {
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

hxhim::Result::SubjectPredicate::SubjectPredicate(hxhim_t *hx, const enum hxhim_op_t op,
                                                   const int range_server, const int status)
    : hxhim::Result::Result(hx, op, range_server, status),
      subject(),
      predicate()
{}

hxhim::Result::SubjectPredicate::~SubjectPredicate() {}

hxhim::Result::Put::Put(hxhim_t *hx,
                         const int range_server, const int status)
    : SubjectPredicate(hx, hxhim_op_t::HXHIM_PUT, range_server, status)
{}

hxhim::Result::Delete::Delete(hxhim_t *hx,
                               const int range_server, const int status)
    : SubjectPredicate(hx, hxhim_op_t::HXHIM_DELETE, range_server, status)
{}

hxhim::Result::Sync::Sync(hxhim_t *hx,
                           const int range_server, const int status)
    : Result(hx, hxhim_op_t::HXHIM_SYNC, range_server, status)
{}

hxhim::Result::Hist::Hist(hxhim_t *hx,
                           const int range_server, const int status)
    : Result(hx, hxhim_op_t::HXHIM_HISTOGRAM, range_server, status),
      histogram(nullptr)
{}

hxhim::Result::Result *hxhim::Result::init(hxhim_t *hx, Message::Response::Response *res, const std::size_t i) {
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

    hxhim::Result::Result *ret = nullptr;
    switch (res->op) {
        case hxhim_op_t::HXHIM_PUT:
            ret = init(hx, static_cast<Message::Response::BPut *>(res), i);
            break;
        case hxhim_op_t::HXHIM_GET:
            ret = init(hx, static_cast<Message::Response::BGet *>(res), i);
            break;
        case hxhim_op_t::HXHIM_GETOP:
            ret = init(hx, static_cast<Message::Response::BGetOp *>(res), i);
            break;
        case hxhim_op_t::HXHIM_DELETE:
            ret = init(hx, static_cast<Message::Response::BDelete *>(res), i);
            break;
        case hxhim_op_t::HXHIM_HISTOGRAM:
            ret = init(hx, static_cast<Message::Response::BHistogram *>(res), i);
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

hxhim::Result::Put *hxhim::Result::init(hxhim_t *hx, Message::Response::BPut *bput, const std::size_t i) {
    hxhim::Result::Put *out = construct<hxhim::Result::Put>(hx, bput->src, bput->statuses[i]);

    out->subject = std::move(bput->orig.subjects[i]);
    out->predicate = std::move(bput->orig.predicates[i]);

    return out;
}

hxhim::Result::Get *hxhim::Result::init(hxhim_t *hx, Message::Response::BGet *bget, const std::size_t i) {
    hxhim::Result::Get *out = construct<hxhim::Result::Get>(hx, bget->src, bget->statuses[i]);

    out->subject = std::move(bget->orig.subjects[i]);
    out->predicate = std::move(bget->orig.predicates[i]);
    if (out->status == HXHIM_SUCCESS) {
        out->object = std::move(bget->objects[i]);
    }
    out->next = nullptr;

    return out;
}

hxhim::Result::GetOp *hxhim::Result::init(hxhim_t *hx, Message::Response::BGetOp *bgetop, const std::size_t i) {
    const int status = bgetop->statuses[i];

    hxhim::Result::GetOp *top = construct<hxhim::Result::GetOp>(hx, bgetop->src, status);

    hxhim::Result::GetOp *prev = nullptr;
    hxhim::Result::GetOp *curr = top;
    for(std::size_t j = 0; j < bgetop->num_recs[i]; j++) {
        curr->subject = std::move(bgetop->subjects[i][j]);
        curr->predicate = std::move(bgetop->predicates[i][j]);

        if (curr->status == HXHIM_SUCCESS) {
            curr->object = std::move(bgetop->objects[i][j]);
        }

        prev = curr;
        curr = construct<hxhim::Result::GetOp>(hx, bgetop->src, status);
        prev->next = curr;
    }

    // drop last item (empty)
    prev->next = nullptr;
    destruct(curr);

    return top;
}

hxhim::Result::Delete *hxhim::Result::init(hxhim_t *hx, Message::Response::BDelete *bdel, const std::size_t i) {
    hxhim::Result::Delete *out = construct<hxhim::Result::Delete>(hx, bdel->src, bdel->statuses[i]);

    out->subject = std::move(bdel->orig.subjects[i]);
    out->predicate = std::move(bdel->orig.predicates[i]);

    return out;
}

hxhim::Result::Sync *hxhim::Result::init(hxhim_t *hx, const int synced) {
    const int rank = hx->p->bootstrap.rank;
    return construct<hxhim::Result::Sync>(hx, rank, synced);
}

hxhim::Result::Hist *hxhim::Result::init(hxhim_t *hx, Message::Response::BHistogram *bhist, const std::size_t i) {
    hxhim::Result::Hist *out = construct<hxhim::Result::Hist>(hx, bhist->src, bhist->statuses[i]);

    if (bhist->statuses[i] == DATASTORE_SUCCESS) {
        out->histogram = bhist->histograms[i];
    }

    bhist->histograms[i] = nullptr;
    return out;
}

/**
 * AddAll
 * Converts an entire response packet into a result list
 *
 * @param results   the result list to insert into
 * @param response  the response packet
 */
void hxhim::Result::AddAll(hxhim_t *hx, hxhim::Results *results,
                           Message::Response::Response *response) {
    for(Message::Response::Response *res = response; res; res = next(res)) {
        for(std::size_t i = 0; i < res->count; i++) {
            results->Add(hxhim::Result::init(hx, res, i));
        }
    }
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
    : results(),
      it(results.end())
{}

hxhim::Results::~Results() {
    for(decltype(results)::value_type &res : results) {
        destruct(res);
    }
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
 * Add
 * Appends a single result node to the end of the list
 * The result type is processed before being inserted
 */
void hxhim::Results::Add(hxhim::Result::Result *response) {
    if (response) {
        // serialize GetOps
        if (response->op == hxhim_op_t::HXHIM_GETOP) {
            for(hxhim::Result::GetOp *get = static_cast<hxhim::Result::GetOp *>(response);
                get; get = get->next) {
                results.push_back(get);
            }
        }
        else {
            results.push_back(response);
        }
    }
}

/**
 * Append
 * Moves and appends the contents of another hxhim::Results into this one.
 * The other list is emptied out.
 */
void hxhim::Results::Append(hxhim::Results *other) {
    if (other) {
        results.insert(results.end(), other->results.begin(), other->results.end());
        other->results.clear();
    }
}

/**
 * ValidIterator
 *
 * @return Whether or not the iterator is at a valid position
 */
bool hxhim::Results::ValidIterator() const {
    return (it != results.end());
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
void hxhim::Results::GoToHead() {
    it = results.begin();
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

    res->res->GoToHead();
    return res->res->ValidIterator()?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * GoToNext
 * Moves the current node to point to the next node in the list
 *
 * @return Whether or not the new position is valid
 */
void hxhim::Results::GoToNext() {
    if (ValidIterator()) {
        it++;
    }
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
    return res->res->ValidIterator()?HXHIM_SUCCESS:HXHIM_ERROR;
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
 * Op
 * Get the operation that was performed to get the current result
 *
 * @param type  (optional) the type of the current result, only valid if this function returns HXHIM_SUCCESS
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int hxhim::Results::Op(enum hxhim_op_t *op) const {
    if (it == results.end()) {
        return HXHIM_ERROR;
    }

    if (op) {
        *op = (*it)->op;
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
    if (it == results.end()) {
        return HXHIM_ERROR;
    }

    if (status) {
        *status = (*it)->status;
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
    if (it == results.end()) {
        return HXHIM_ERROR;
    }

    if (range_server) {
        *range_server = (*it)->range_server;
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
    if (it == results.end()) {
        return HXHIM_ERROR;
    }

    Result::Result *res = *it;
    if ((res->op != hxhim_op_t::HXHIM_PUT)    &&
        (res->op != hxhim_op_t::HXHIM_GET)    &&
        (res->op != hxhim_op_t::HXHIM_GETOP)  &&
        (res->op != hxhim_op_t::HXHIM_DELETE)) {
        return HXHIM_ERROR;
    }

    hxhim::Result::SubjectPredicate *sp = static_cast<hxhim::Result::SubjectPredicate *>(res);
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
    if (it == results.end()) {
        return HXHIM_ERROR;
    }

    Result::Result *res = *it;
    if ((res->op != hxhim_op_t::HXHIM_PUT)    &&
        (res->op != hxhim_op_t::HXHIM_GET)    &&
        (res->op != hxhim_op_t::HXHIM_GETOP)  &&
        (res->op != hxhim_op_t::HXHIM_DELETE)) {
        return HXHIM_ERROR;
    }

    hxhim::Result::SubjectPredicate *sp = static_cast<hxhim::Result::SubjectPredicate *>(res);
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
    if (it == results.end()) {
        return HXHIM_ERROR;
    }

    Result::Result *res = *it;
    if (((res->op    != hxhim_op_t::HXHIM_GET)    &&
         (res->op    != hxhim_op_t::HXHIM_GETOP)) ||
        (res->status != HXHIM_SUCCESS))            {
        return HXHIM_ERROR;
    }

    hxhim::Result::Get *get = static_cast<hxhim::Result::Get *>(res);
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
    if (it == results.end()) {
        return HXHIM_ERROR;
    }

    Result::Result *res = *it;
    if ((res->op     != hxhim_op_t::HXHIM_HISTOGRAM) ||
        (res->status != HXHIM_SUCCESS))               {
        return HXHIM_ERROR;
    }

    hxhim::Result::Hist *hist = static_cast<hxhim::Result::Hist *>(res);
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
    if (it == results.end()) {
        return HXHIM_ERROR;
    }

    Result::Result *res = *it;
    if ((res->op     != hxhim_op_t::HXHIM_HISTOGRAM) ||
        (res->status != HXHIM_SUCCESS))               {
        return HXHIM_ERROR;
    }

    if (hist) {
        hxhim::Result::Hist *h = static_cast<hxhim::Result::Hist *>(res);
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
int hxhim::Results::Timestamps(struct hxhim::Result::Timestamps **timestamps) const {
    if (it == results.end()) {
        return HXHIM_ERROR;
    }

    if (timestamps) {
        *timestamps = &(*it)->timestamps;
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
