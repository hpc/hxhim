#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"
#include "utils/Stats.hpp"
#include "utils/macros.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * new_request
 * Creates a new Message::Request packet
 * and fills in the allocate timestamp
 *
 * @param hx      the HXHIM session
 * @param queue   the queue to push into
 */
template <typename Request_t,
          typename = enable_if_t <is_child_of <Message::Request::Request, Request_t>::value> >
void new_request(hxhim_t *hx, hxhim::QueueTarget<Request_t> &queue) {
    ::Stats::Chronostamp allocate;
    allocate.start = ::Stats::now();
    queue.push_back(construct<Request_t>(hx->p->queues.max_per_request.ops));
    queue.back()->timestamps.allocate = allocate;
    queue.back()->timestamps.allocate.end = ::Stats::now();
}

/**
 * setup_packet
 * Checks if the queue is empty or if the last queued packet
 * is full and creates a new request packet if necessary.
 *
 * @param hx      the HXHIM session
 * @param queue   the queue to push into
 * @return pointer to a packet with space for a new set of data
 */
template <typename Request_t,
          typename = enable_if_t <is_child_of <Message::Request::Request, Request_t>::value> >
Request_t *setup_packet(hxhim_t *hx, hxhim::QueueTarget<Request_t> &queue, const size_t additional_size) {
    if (queue.empty()                                                   ||
        // last packet doesn't have an empty slot
        ((*queue.rbegin())->count >= hx->p->queues.max_per_request.ops) ||
        // has slots, but serialized buffer would be too big
        (hx->p->queues.max_per_request.size &&
         (((*queue.rbegin())->size() + additional_size) > hx->p->queues.max_per_request.size))) {
        new_request(hx, queue);
    }

    return *(queue.rbegin());
}

/**
 * get_packet
 * hashes the subject + predicate and sets
 * up the queue for inserting data.
 *
 * This function only exists to reduce repetition of code.
 *
 * @param hx         the HXHIM session
 * @param queue      the queue to push into
 * @param subject    the subject to hash
 * @param predicate  the predicate to hash
 * @param insert     the insert timerstamps - only start is filled
 * @return pointer to a packet with space for a new set of data
 */
template <typename Request_t,
          typename = enable_if_t <is_child_of <Message::Request::Request, Request_t>::value> >
Request_t *get_packet(hxhim_t *hx,
                      hxhim::Queues<Request_t> &queues,
                      const Blob &subject, const Blob &predicate) {
    ::Stats::Chronostamp hash;
    hash.start = ::Stats::now();

    // figure out where this triple is going
    const int rs_id = hxhim_hash(hx,
                                 subject.data(),  subject.size(),
                                 predicate.data(), predicate.size());

    hash.end = ::Stats::now();
    if (rs_id < 0) {
        return nullptr;
    }

    mlog(HXHIM_CLIENT_DBG, "Foreground Insert into queue");

    ::Stats::Chronopoint insert_start = ::Stats::now();

    // set up the packet this triple should be placed into
    Request_t *req = setup_packet(hx, queues[rs_id], subject.pack_size(true) + predicate.pack_size(true));
    req->timestamps.reqs[req->count].hash = hash;
    req->timestamps.reqs[req->count].insert.start = insert_start;
    return req;
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
 * @param object         the object to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::PutImpl(hxhim_t *hx,
                   hxhim::Queues<Message::Request::BPut> &puts,
                   Blob subject,
                   Blob predicate,
                   Blob object,
                   const hxhim_put_permutation_t permutations) {
    mlog(HXHIM_CLIENT_INFO, "Foreground PUT Start (%p, %p, %p)", subject.data(), predicate.data(), object.data());

    for(std::size_t i = 0; i < HXHIM_PUT_PERMUTATIONS_COUNT; i++) {
        if (!(permutations & HXHIM_PUT_PERMUTATIONS[i])) {
            continue;
        }

        Blob *sub  = nullptr;
        Blob *pred = nullptr;
        Blob *obj  = nullptr;

        switch (HXHIM_PUT_PERMUTATIONS[i]) {
            case HXHIM_PUT_SPO:
                sub  = &subject;
                pred = &predicate;
                obj  = &object;
                break;
            case HXHIM_PUT_SOP:
                sub  = &subject;
                pred = &object;
                obj  = &predicate;
                break;
            case HXHIM_PUT_PSO:
                sub  = &predicate;
                pred = &subject;
                obj  = &object;
                break;
            case HXHIM_PUT_POS:
                sub  = &predicate;
                pred = &object;
                obj  = &subject;
                break;
            case HXHIM_PUT_OSP:
                sub  = &object;
                pred = &subject;
                obj  = &predicate;
                break;
            case HXHIM_PUT_OPS:
                sub  = &object;
                pred = &predicate;
                obj  = &subject;
                break;
            default:
                return HXHIM_ERROR;
        }

        ::Stats::Chronostamp hash;
        hash.start = ::Stats::now();

        // figure out where this triple is going
        const int rs_id = hxhim_hash(hx,
                                     sub->data(),  sub->size(),
                                     pred->data(), pred->size());

        hash.end = ::Stats::now();
        if (rs_id < 0) {
            return HXHIM_ERROR;
        }

        mlog(HXHIM_CLIENT_DBG, "Foreground PUT Insert SPO into queue");

        ::Stats::Chronostamp insert;
        insert.start = ::Stats::now();

        // add the triple to the last packet in the queue
        Message::Request::BPut *put = setup_packet(hx, puts[rs_id],
                                                   sub->pack_size(true) +
                                                   pred->pack_size(true) +
                                                   obj->pack_size(true));
        put->add(*sub, *pred, *obj);
        hx->p->queues.puts.count++; // mutex is locked by caller

        put->timestamps.reqs[put->count - 1].hash = hash;
        put->timestamps.reqs[put->count - 1].insert = insert;
        put->timestamps.reqs[put->count - 1].insert.end = ::Stats::now();
    }

    // trigger background PUTs in higher scope in order to allow for all BPUTs to queue up before flushing

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
                   hxhim::Queues<Message::Request::BGet> &gets,
                   Blob subject,
                   Blob predicate,
                   enum hxhim_data_t object_type) {
    mlog(HXHIM_CLIENT_DBG, "GET Start");

    Message::Request::BGet *get = get_packet(hx, gets,
                                             subject, predicate);
    if (!get) {
        return HXHIM_ERROR;
    }

    // add the data to the packet
    get->add(subject, predicate, object_type);

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
                     hxhim::Queues<Message::Request::BGetOp> &getops,
                     Blob subject,
                     Blob predicate,
                     enum hxhim_data_t object_type,
                     std::size_t num_records, enum hxhim_getop_t op) {
    mlog(HXHIM_CLIENT_DBG, "GETOP Start");

    Message::Request::BGetOp *getop = get_packet(hx, getops,
                                                 subject, predicate);
    if (!getop) {
        return HXHIM_ERROR;
    }

    // add the data to the packet
    getop->add(subject, predicate, object_type, num_records, op);
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
                      hxhim::Queues<Message::Request::BDelete> &dels,
                      Blob subject,
                      Blob predicate) {
    mlog(HXHIM_CLIENT_DBG, "DELETE Start");

    Message::Request::BDelete *del = get_packet(hx, dels,
                                                subject, predicate);
    if (!del) {
        return HXHIM_ERROR;
    }

    // add the data to the packet
    del->add(subject, predicate);

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
 * @param rs_id   the range server id - value checked by caller
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::HistogramImpl(hxhim_t *hx,
                         hxhim::Queues<Message::Request::BHistogram> &hists,
                         const int rs_id,
                         const char *name, const std::size_t name_len) {
    mlog(HXHIM_CLIENT_DBG, "HISTOGRAM Start");

    ::Stats::Chronostamp hash;
    hash.start = ::Stats::now();
    hash.end = ::Stats::now();

    // rs_id is checked by caller
    // name does not need to be checked here/by caller

    mlog(HXHIM_CLIENT_DBG, "Foreground HISTOGRAM Insert SPO into queue");

    ::Stats::Chronostamp insert;
    insert.start = ::Stats::now();

    Blob n = ReferenceBlob((void *) name, name_len, hxhim_data_t::HXHIM_DATA_BYTE);

    // add the data to the packet
    Message::Request::BHistogram *hist = setup_packet(hx, hists[rs_id], n.pack_size(false));
    hist->add(ReferenceBlob((void *) name, name_len, hxhim_data_t::HXHIM_DATA_BYTE));

    hist->timestamps.reqs[hist->count - 1].hash = hash;
    hist->timestamps.reqs[hist->count - 1].insert = insert;
    hist->timestamps.reqs[hist->count - 1].insert.end = ::Stats::now();

    mlog(HXHIM_CLIENT_DBG, "Histogram Completed");
    return HXHIM_SUCCESS;
}
