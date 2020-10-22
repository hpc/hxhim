#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"
#include "utils/Stats.hpp"
#include "utils/macros.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * new_request
 * Creates a new Transport::Request packet
 * and fills in the allocate timestamp
 *
 * @param hx      the HXHIM session
 * @param queue   the queue to push into
 *
 */
template <typename Request_t,
          typename = enable_if_t <is_child_of <Transport::Request::Request, Request_t>::value> >
void new_request(hxhim_t *hx, hxhim::QueueTarget<Request_t> &queue) {
    ::Stats::Chronostamp allocate;
    allocate.start = ::Stats::now();
    queue.push_back(construct<Request_t>(hx->p->queues.max_ops_per_send));
    queue.back()->timestamps.allocate = allocate;
    queue.back()->timestamps.allocate.end = ::Stats::now();
}

/**
 * PutImpl
 * Add a PUT into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param hx             the HXHIM session
 * @param puts           the queue to place the PUT in
 * @param subject        the subject to put
 * @param predicate      the prediate to put
 * @param object_type    the type of the object
 * @param object         the object to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::PutImpl(hxhim_t *hx,
                   hxhim::Queue<Transport::Request::BPut> &puts_queue,
                   Blob subject,
                   Blob predicate,
                   enum hxhim_object_type_t object_type,
                   Blob object) {
    mlog(HXHIM_CLIENT_INFO, "Foreground PUT Start (%p, %p, %p)", subject.data(), predicate.data(), object.data());

    ::Stats::Chronostamp hash;
    hash.start = ::Stats::now();

    // figure out where this triple is going
    const int rs_id = hx->p->hash.func(hx,
                                       subject.data(),   subject.size(),
                                       predicate.data(), predicate.size(),
                                       hx->p->hash.args);

    hash.end = ::Stats::now();
    if (rs_id < 0) {
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_DBG, "Foreground PUT Insert SPO into queue");

    ::Stats::Chronostamp insert;
    insert.start = ::Stats::now();

    #if ASYNC_PUTS
    std::unique_lock<std::mutex> lock(hx->p->queues.puts.mutex);
    #endif

    // find the queue this triple should be placed on
    QueueTarget<Transport::Request::BPut> &puts = puts_queue[rs_id];

    if (puts.empty()                                                ||
        ((*puts.rbegin())->count >= hx->p->queues.max_ops_per_send)) { // last packet doesn't have space
        new_request(hx, puts);
    }

    // add the triple to the last packet in the queue
    Transport::Request::BPut *put = *(puts.rbegin());

    put->subjects[put->count] = subject;
    put->predicates[put->count] = predicate;
    put->object_types[put->count] = object_type;
    put->objects[put->count] = object;

    put->orig.subjects[put->count] = subject.data();
    put->orig.predicates[put->count] = predicate.data();

    put->timestamps.reqs[put->count].hash = hash;
    put->timestamps.reqs[put->count].insert = insert;

    put->count++;

    hx->p->queues.puts.count++;

    put->timestamps.reqs[put->count - 1].insert.end = ::Stats::now();

    // do not trigger background PUTs here in order to allow for all BPUTs to queue up before flushing

    mlog(HXHIM_CLIENT_DBG, "Foreground PUT Completed");
    return HXHIM_SUCCESS;
}

/**
 * GetImpl
 * Add a GET into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param hx             the HXHIM session
 * @param gets           the queue to place the GET in
 * @param subject        the subject to put
 * @param predicate      the prediate to put
 * @param object_type    the type of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetImpl(hxhim_t *hx,
                   hxhim::Queue<Transport::Request::BGet> &gets_queue,
                   Blob subject,
                   Blob predicate,
                   enum hxhim_object_type_t object_type) {
    mlog(HXHIM_CLIENT_DBG, "GET Start");

    ::Stats::Chronostamp hash;
    hash.start = ::Stats::now();

    // figure out where this triple is going
    const int rs_id = hx->p->hash.func(hx,
                                       subject.data(),   subject.size(),
                                       predicate.data(), predicate.size(),
                                       hx->p->hash.args);

    hash.end = ::Stats::now();
    if (rs_id < 0) {
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_DBG, "Foreground GET Insert SPO into queue");

    ::Stats::Chronostamp insert;
    insert.start = ::Stats::now();

    // find the queue this triple should be placed on
    QueueTarget<Transport::Request::BGet> &gets = gets_queue[rs_id];
    if (gets.empty()                                                ||
        ((*gets.rbegin())->count >= hx->p->queues.max_ops_per_send)) { // last packet doesn't have space
        new_request(hx, gets);
    }

    // add the triple to the last packet in the queue
    Transport::Request::BGet *get = *(gets.rbegin());

    get->subjects[get->count] = subject;
    get->predicates[get->count] = predicate;
    get->object_types[get->count] = object_type;

    get->orig.subjects[get->count] = subject.data();
    get->orig.predicates[get->count] = predicate.data();

    get->timestamps.reqs[get->count].hash = hash;
    get->timestamps.reqs[get->count].insert = insert;

    get->count++;

    get->timestamps.reqs[get->count - 1].insert.end = ::Stats::now();

    mlog(HXHIM_CLIENT_DBG, "GET Completed");
    return HXHIM_SUCCESS;
}

/**
 * GetOpImpl
 * Add a GETOP into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param hx             the HXHIM session
 * @param getops         the queue to place the GETOP in
 * @param subject        the subject to put
 * @param predicate      the prediate to put
 * @param object_type    the type of the object
 * @param num_records    the number of records to get
 * @param op             the operation to run
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetOpImpl(hxhim_t *hx,
                     hxhim::Queue<Transport::Request::BGetOp> &getops_queue,
                     Blob subject,
                     Blob predicate,
                     enum hxhim_object_type_t object_type,
                     std::size_t num_records, enum hxhim_getop_t op) {
    mlog(HXHIM_CLIENT_DBG, "GETOP Start");

    ::Stats::Chronostamp hash;
    hash.start = ::Stats::now();

    // figure out where this triple is going
    const int rs_id = hx->p->hash.func(hx,
                                       subject.data(),   subject.size(),
                                       predicate.data(), predicate.size(),
                                       hx->p->hash.args);

    hash.end = ::Stats::now();
    if (rs_id < 0) {
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_DBG, "Foreground GETOP Insert SPO into queue");

    ::Stats::Chronostamp insert;
    insert.start = ::Stats::now();

    // find the queue this triple should be placed on
    QueueTarget<Transport::Request::BGetOp> &getops = getops_queue[rs_id];
    if (getops.empty()                                                ||
        ((*getops.rbegin())->count >= hx->p->queues.max_ops_per_send)) { // last packet doesn't have space
        new_request(hx, getops);
    }

    // add the triple to the last packet in the queue
    Transport::Request::BGetOp *getop = *(getops.rbegin());

    getop->subjects[getop->count] = subject;
    getop->predicates[getop->count] = predicate;
    getop->object_types[getop->count] = object_type;
    getop->num_recs[getop->count] = num_records;
    getop->ops[getop->count] = op;

    getop->timestamps.reqs[getop->count].hash = hash;
    getop->timestamps.reqs[getop->count].insert = insert;

    getop->count++;

    getop->timestamps.reqs[getop->count - 1].insert.end = ::Stats::now();

    mlog(HXHIM_CLIENT_DBG, "GETOP Completed");
    return HXHIM_SUCCESS;
}

/**
 * DeleteImpl
 * Add a DELETE into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param hx           the HXHIM session
 * @param dels         the queue to place the DELETE in
 * @param subject      the subject to delete
 * @param prediate     the prediate to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::DeleteImpl(hxhim_t *hx,
                      hxhim::Queue<Transport::Request::BDelete> &dels_queue,
                      Blob subject,
                      Blob predicate) {
    mlog(HXHIM_CLIENT_DBG, "DELETE Start");

    ::Stats::Chronostamp hash;
    hash.start = ::Stats::now();

    // figure out where this triple is going
    const int rs_id = hx->p->hash.func(hx,
                                       subject.data(),   subject.size(),
                                       predicate.data(), predicate.size(),
                                       hx->p->hash.args);

    hash.end = ::Stats::now();
    if (rs_id < 0) {
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_DBG, "Foreground DELETE Insert SPO into queue");

    ::Stats::Chronostamp insert;
    insert.start = ::Stats::now();

    // find the queue this triple should be placed on
    QueueTarget<Transport::Request::BDelete> &dels = dels_queue[rs_id];
    if (dels.empty()                                                ||
        ((*dels.rbegin())->count >= hx->p->queues.max_ops_per_send)) { // last packet doesn't have space
        new_request(hx, dels);
    }

    // add the triple to the last packet in the queue
    Transport::Request::BDelete *del = *(dels.rbegin());

    del->subjects[del->count] = subject;
    del->predicates[del->count] = predicate;

    del->orig.subjects[del->count] = subject.data();
    del->orig.predicates[del->count] = predicate.data();

    del->timestamps.reqs[del->count].hash = hash;
    del->timestamps.reqs[del->count].insert = insert;

    del->count++;

    del->timestamps.reqs[del->count - 1].insert.end = ::Stats::now();

    mlog(HXHIM_CLIENT_DBG, "Delete Completed");
    return HXHIM_SUCCESS;
}

/**
 * HistogramImpl
 * Add a HISTOGRAM into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param hx      the HXHIM session
 * @param hists   the queue to place the HISTOGRAM in
 * @param ds_id   the datastore id - value checked by caller
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::HistogramImpl(hxhim_t *hx,
                         hxhim::Queue<Transport::Request::BHistogram> &hists_queue,
                         const int rs_id) {
    mlog(HXHIM_CLIENT_DBG, "HISTOGRAM Start");

    ::Stats::Chronostamp hash;
    hash.start = ::Stats::now();
    hash.end = ::Stats::now();

    if (rs_id < 0) {
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_DBG, "Foreground HISTOGRAM Insert SPO into queue");

    ::Stats::Chronostamp insert;
    insert.start = ::Stats::now();

    // find the queue this triple should be placed on
    QueueTarget<Transport::Request::BHistogram> &hists = hists_queue[rs_id];
    if (hists.empty()                                                ||
        ((*hists.rbegin())->count >= hx->p->queues.max_ops_per_send)) { // last packet doesn't have space
        new_request(hx, hists);
    }

    // add the triple to the last packet in the queue
    Transport::Request::BHistogram *hist = *(hists.rbegin());

    hist->timestamps.reqs[hist->count].hash = hash;
    hist->timestamps.reqs[hist->count].insert = insert;

    hist->count++;

    hist->timestamps.reqs[hist->count - 1].insert.end = ::Stats::now();

    mlog(HXHIM_CLIENT_DBG, "Histogram Completed");
    return HXHIM_SUCCESS;
}
