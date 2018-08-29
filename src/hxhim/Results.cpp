#include <cstdlib>
#include <cstring>

#include "hxhim/Results.hpp"
#include "hxhim/Results_private.hpp"
#include "hxhim/private.hpp"

namespace hxhim {

Results::Result::Result(hxhim_result_type t, FixedBufferPool *buffers)
    : type(t),
      buffers(buffers)
{}

Results::Result::~Result() {}

hxhim_result_type_t Results::Result::GetType() const {
    return type;
}

int Results::Result::GetDatastore() const {
    return datastore;
}

int Results::Result::GetStatus() const {
    return status;
}

Results::Put::Put(hxhim_t *hx, Transport::Response::Put *put)
    : Result(hxhim_result_type::HXHIM_RESULT_PUT, hx->p->memory_pools.buffers)
{
    if (put) {
        datastore = hxhim::datastore::get_id(hx, put->src, put->ds_offset);
        status = put->status;
    }
}

Results::Put::Put(hxhim_t *hx, Transport::Response::BPut *bput, const std::size_t i)
    : Result(hxhim_result_type::HXHIM_RESULT_PUT, hx->p->memory_pools.buffers)
{
    if (bput && (i < bput->count)) {
        datastore = hxhim::datastore::get_id(hx, bput->src, bput->ds_offsets[i]);
        status = bput->statuses[i];
    }
}

Results::Put::~Put() {}

Results::Get::Get(hxhim_t *hx)
    : Result(HXHIM_RESULT_GET, hx->p->memory_pools.buffers),
      sub(nullptr), sub_len(0),
      pred(nullptr), pred_len(0),
      obj_type(), obj(nullptr), obj_len(0)
{}

Results::Get::Get(hxhim_t *hx, Transport::Response::Get *get)
    : Get(hx)
{
    if (get) {
        datastore = hxhim::datastore::get_id(hx, get->src, get->ds_offset);
        status = get->status;
        sub = get->subject;
        get->subject = nullptr;
        sub_len = get->subject_len;
        pred = get->predicate;
        get->predicate = nullptr;
        pred_len = get->predicate_len;
        obj_type = get->object_type;
        obj = get->object;
        get->object = nullptr;
        obj_len = get->object_len;
    }
}

Results::Get::Get(hxhim_t *hx, Transport::Response::BGet *bget, const std::size_t i)
    : Get(hx)
{
    if (bget && (i < bget->count)) {
        datastore = hxhim::datastore::get_id(hx, bget->src, bget->ds_offsets[i]);
        status = bget->statuses[i];
        sub = std::move(bget->subjects[i]);
        bget->subjects[i] = nullptr;
        sub_len = std::move(bget->subject_lens[i]);
        pred = std::move(bget->predicates[i]);
        bget->predicates[i] = nullptr;
        pred_len = std::move(bget->predicate_lens[i]);
        obj_type = std::move(bget->object_types[i]);
        obj = std::move(bget->objects[i]);
        bget->objects[i] = nullptr;
        obj_len = std::move(bget->object_lens[i]);
    }
}

Results::Get::Get(hxhim_t *hx, Transport::Response::BGetOp *bgetop, const std::size_t i)
    : Get(hx)
{
    if (bgetop && (i < bgetop->count)) {
        datastore = hxhim::datastore::get_id(hx, bgetop->src, bgetop->ds_offsets[i]);
        status = bgetop->statuses[i];
        sub = std::move(bgetop->subjects[i]);
        bgetop->subjects[i] = nullptr;
        sub_len = std::move(bgetop->subject_lens[i]);
        pred = std::move(bgetop->predicates[i]);
        bgetop->predicates[i] = nullptr;
        pred_len = std::move(bgetop->predicate_lens[i]);
        obj_type = std::move(bgetop->object_types[i]);
        obj = std::move(bgetop->objects[i]);
        bgetop->objects[i] = nullptr;
        obj_len = std::move(bgetop->object_lens[i]);
    }
}

Results::Get::~Get() {
    buffers->release(sub);
    buffers->release(pred);
    buffers->release(obj);
}

hxhim_type_t Results::Get::GetObjectType() const {
    return obj_type;
}

int Results::Get::GetSubject(void **subject, std::size_t *subject_len) const {
    if (subject) {
        *subject = sub;
    }

    if (subject_len) {
        *subject_len = sub_len;
    }

    return HXHIM_SUCCESS;
}

int Results::Get::GetPredicate(void **predicate, std::size_t *predicate_len) const {
    if (predicate) {
        *predicate = pred;
    }

    if (predicate_len) {
        *predicate_len = pred_len;
    }

    return HXHIM_SUCCESS;
}

int Results::Get::GetObject(void **object, std::size_t *object_len) const {
    if (object) {
        *object = obj;
    }

    if (object_len) {
        *object_len = obj_len;
    }

    return HXHIM_SUCCESS;
}

Results::Delete::Delete(hxhim_t *hx, Transport::Response::Delete *del)
    : Result(hxhim_result_type::HXHIM_RESULT_DEL, hx->p->memory_pools.buffers)
{
    if (del) {
        datastore = hxhim::datastore::get_id(hx, del->src, del->ds_offset);
        status = del->status;
    }
}

Results::Delete::Delete(hxhim_t *hx, Transport::Response::BDelete *bdel, const std::size_t i)
    : Result(hxhim_result_type::HXHIM_RESULT_DEL, hx->p->memory_pools.buffers)
{
    if (bdel && (i < bdel->count)) {
        datastore = hxhim::datastore::get_id(hx, bdel->src, bdel->ds_offsets[i]);
        status = bdel->statuses[i];
    }
}

Results::Delete::~Delete() {}

Results::Sync::Sync(hxhim_t *hx, const int ds_offset, const int synced)
    : Result(hxhim_result_type::HXHIM_RESULT_SYNC, hx->p->memory_pools.buffers)
{
    datastore = hxhim::datastore::get_id(hx, hx->p->bootstrap.rank, ds_offset);
    status = synced;
}

Results::Sync::~Sync() {}

Results::Histogram::Histogram(hxhim_t *hx, Transport::Response::Histogram *hist)
    : Result(hxhim_result_type::HXHIM_RESULT_HISTOGRAM, hx->p->memory_pools.buffers),
      buckets(nullptr),
      counts(nullptr),
      size(0)
{
    if (hist) {
        datastore = hxhim::datastore::get_id(hx, hist->src, hist->ds_offset);
        buckets = hist->hist.buckets;
        hist->hist.buckets = nullptr;
        counts = hist->hist.counts;
        hist->hist.counts = nullptr;
        size = hist->hist.size;
        hist->hist.size = 0;

        status = hist->status;
    }
}

Results::Histogram::Histogram(hxhim_t *hx, Transport::Response::BHistogram *bhist, const std::size_t i)
    : Result(hxhim_result_type::HXHIM_RESULT_HISTOGRAM, hx->p->memory_pools.buffers),
      buckets(nullptr),
      counts(nullptr),
      size(0)
{
    if (bhist && (i < bhist->count)) {
        datastore = hxhim::datastore::get_id(hx, bhist->src, bhist->ds_offsets[i]);
        buckets = bhist->hists[i].buckets;
        counts = bhist->hists[i].counts;
        size = bhist->hists[i].size;
        status = bhist->statuses[i];
    }
}

Results::Histogram::~Histogram() {}

int Results::Histogram::GetBuckets(double **b) const {
    if (!b) {
        return HXHIM_ERROR;
    }

    *b = buckets;

    return HXHIM_SUCCESS;
}

int Results::Histogram::GetCounts(std::size_t **c) const {
    if (!c) {
        return HXHIM_ERROR;
    }

    *c = counts;

    return HXHIM_SUCCESS;
}

int Results::Histogram::GetSize(std::size_t *s) const {
    if (!s) {
        return HXHIM_ERROR;
    }

    *s = size;

    return HXHIM_SUCCESS;
}

Results::Results(hxhim_t *hx)
    : hx(hx),
      results(),
      curr(results.end())
{}

Results::~Results() {
    curr = results.end();

    for(Result *res : results) {
        switch (res->GetType()) {
            case HXHIM_RESULT_PUT:
                hx->p->memory_pools.result->release(static_cast<Put *>(res));
                break;
            case HXHIM_RESULT_GET:
                hx->p->memory_pools.result->release(static_cast<Get *>(res));
                break;
            case HXHIM_RESULT_DEL:
                hx->p->memory_pools.result->release(static_cast<Delete *>(res));
                break;
            case HXHIM_RESULT_SYNC:
                hx->p->memory_pools.result->release(static_cast<Sync *>(res));
                break;
            case HXHIM_RESULT_HISTOGRAM:
                hx->p->memory_pools.result->release(static_cast<Histogram *>(res));
                break;
            default:
                delete res;
                break;
        }
    }

    results.clear();
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
    hxhim_results_t *ret = hx->p->memory_pools.buffers->acquire<hxhim_results_t>();
    ret->res = res;
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
 * hxhim_results_
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
        *type = curr->GetType();
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
        *error = curr->GetStatus();
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
        *datastore = curr->GetDatastore();
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
    if (curr->GetType() == hxhim_result_type::HXHIM_RESULT_GET) {
        if (object_type) {
            *object_type = static_cast<hxhim::Results::Get *>(curr)->GetObjectType();
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
    if (curr->GetType() == hxhim_result_type::HXHIM_RESULT_GET) {
        return static_cast<hxhim::Results::Get *>(curr)->GetSubject(subject, subject_len);
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
    if (curr->GetType() == hxhim_result_type::HXHIM_RESULT_GET) {
        return static_cast<hxhim::Results::Get *>(curr)->GetPredicate(predicate, predicate_len);
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
    if (curr->GetType() == hxhim_result_type::HXHIM_RESULT_GET) {
        return static_cast<hxhim::Results::Get *>(curr)->GetObject(object, object_len);
    }

    return HXHIM_ERROR;
}

/**
 * operator delete
 * Destroys the results returned from an operation
 *
 * @param hx    the HXHIM session
 * @param res   pointer to a set of results
 */
void hxhim_results_destroy(hxhim_t *hx, hxhim::Results *res) {
    if (res) {
        res->~Results();
        hx->p->memory_pools.results->release(res);
    }
}

/**
 * hxhim_results_destroy
 * Destroys the contents of a results struct and the object it is pointing to
 *
 * @param res A list of results
 */
void hxhim_results_destroy(hxhim_t *hx, hxhim_results_t *res) {
    if (res) {
        hxhim_results_destroy(hx, res->res);
        delete res;
    }
}
