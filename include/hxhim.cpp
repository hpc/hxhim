#include <algorithm>
#include <cmath>

#include "mlog2.h"
#include "mlogfacs2.h"

#include "hxhim.h"
#include "hxhim.hpp"
#include "hxhim_config.h"
#include "hxhim_private.hpp"
#include "return_private.hpp"
#include "triplestore.hpp"

/**
 * put_core
 * The core functionality for putting a set of SPO triples into MDHIM
 *
 * @param hx      the HXHIM context
 * @param head    the head of the list of SPO triple batches to send
 * @param count   the number of triples in each batch
 * @return Pointer to return value wrapper
 */
static hxhim::Return *put_core(hxhim_t *hx, hxhim::PutData *head, const std::size_t count) {
    if (!count) {
        return nullptr;
    }

    static const std::size_t multiplier = 4;
    const std::size_t total = multiplier * count;

    void **keys = new void *[total]();
    std::size_t *key_lens = new std::size_t[total]();
    void **values = new void *[total]();
    std::size_t *value_lens = new std::size_t[total]();

    std::size_t offset = 0;

    for(std::size_t i = 0; i < count; i++) {
        sp_to_key(head->subjects[i], head->subject_lens[i], head->predicates[i], head->predicate_lens[i], &keys[offset], &key_lens[offset]);
        values[offset] = head->objects[i];
        value_lens[offset] = head->object_lens[i];
        offset++;

        sp_to_key(head->subjects[i], head->subject_lens[i], head->objects[i], head->object_lens[i], &keys[offset], &key_lens[offset]);
        values[offset] = head->predicates[i];
        value_lens[offset] = head->predicate_lens[i];
        offset++;

        sp_to_key(head->predicates[i], head->predicate_lens[i], head->objects[i], head->object_lens[i], &keys[offset], &key_lens[offset]);
        values[offset] = head->subjects[i];
        value_lens[offset] = head->subject_lens[i];
        offset++;

        sp_to_key(head->predicates[i], head->predicate_lens[i], head->subjects[i], head->subject_lens[i], &keys[offset], &key_lens[offset]);
        values[offset] = head->objects[i];
        value_lens[offset] = head->object_lens[i];
        offset++;
    }

    // PUT the batch
    hxhim::Return *ret = new hxhim::Return(hxhim_work_op::HXHIM_PUT, mdhim::BPut(hx->p->md, nullptr, keys, key_lens, values, value_lens, total));

    // cleanup
    for(std::size_t i = 0; i < total; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;
    delete [] key_lens;
    delete [] values;
    delete [] value_lens;

    return ret;
}

/**
 * backgroundPUT
 * The thread that runs when the number of full batches crosses the watermark
 *
 * @param args   hx typecast to void *
 */
static void backgroundPUT(void *args) {
    hxhim_t *hx = (hxhim_t *)args;

    while (hx->p->running) {
        hxhim::PutData *head = nullptr;    // the first batch of PUTs to process

        bool force = false;                // whether or not FlushPuts was called
        hxhim::PutData *last = nullptr;    // pointer to the last batch of PUTs; only valid if force is true
        std::size_t last_count = 0;        // number of SPO triples in the last batch

        // hold unsent.mutex just long enough to move queued PUTs to send queue
        {
            hxhim::Unsent<hxhim::PutData> &unsent = hx->p->puts;
            std::unique_lock<std::mutex> lock(unsent.mutex);
            while (hx->p->running && (unsent.full_batches < hx->p->watermark) && !unsent.force) {
                unsent.start_processing.wait(lock, [&]() -> bool { return !hx->p->running || (unsent.full_batches >= hx->p->watermark) || unsent.force; });
            }

            // record whether or not this loop was forced, since the lock is not held
            force = unsent.force;

            if (hx->p->running) {
                unsent.full_batches = 0;

                // nothing to do
                if (!unsent.head) {
                    if (force) {
                        unsent.force = false;
                        unsent.done_processing.notify_all();
                    }
                    continue;
                }

                if (force) {
                    // current batch is not the last one
                    if (unsent.head->next) {
                        head = unsent.head;
                        last = unsent.tail;

                        // disconnect the tail from the previous batches
                        last->prev->next = nullptr;
                    }
                    // current batch is the last one
                    else {
                        head = nullptr;
                        last = unsent.head;
                    }

                    // keep track of the size of the last batch
                    last_count = unsent.last_count;
                    unsent.last_count = 0;
                    unsent.force = false;

                    // remove all of the PUTs from the queue
                    unsent.head = nullptr;
                    unsent.tail = nullptr;
                }
                // not forced
                else {
                    // current batch is not the last one
                    if (unsent.head->next) {
                        head = unsent.head;
                        unsent.head = unsent.tail;

                        // disconnect the tail from the previous batches
                        unsent.tail->prev->next = nullptr;
                    }
                    // current batch is the last one
                    else {
                        continue;
                    }
                }
            }
        }

        // process the queued PUTs
        while (hx->p->running && head) {
            // process the batch and save the results
            hxhim::Return *ret = put_core(hx, head, HXHIM_MAX_BULK_PUT_OPS);

            // go to the next batch
            hxhim::PutData *next = head->next;
            delete head;
            head = next;

            {
                std::unique_lock<std::mutex> lock(hx->p->results_mutex);
                // TODO: hxhim::Return needs to be changed so that appending works better
                hxhim::Return *tail = ret;
                while (tail->Next()) tail = tail->Next();
                tail->Next(hx->p->results);
                hx->p->results = ret;
            }
        }

        // if this flush was forced, notify FlushPuts
        if (force) {
            if (hx->p->running) {
                // process the batch
                hxhim::Return *ret = put_core(hx, last, last_count);

                delete last;
                last = nullptr;

                {
                    std::unique_lock<std::mutex> lock(hx->p->results_mutex);
                    // TODO: hxhim::Return needs to be changed so that appending works better
                    hxhim::Return *tail = ret;
                    while (tail->Next()) tail = tail->Next();
                    tail->Next(hx->p->results);
                    hx->p->results = ret;
                }
            }

            hxhim::Unsent<hxhim::PutData> &unsent = hx->p->puts;
            std::unique_lock<std::mutex> lock(unsent.mutex);
            unsent.done_processing.notify_all();
        }

        // clean up in case previous loop stopped early
        hxhim::clean(head);
        delete last;
    }
}

/**
 * Open
 * Start a HXHIM session
 *
 * @param hx             the HXHIM session
 * @param bootstrap_comm the MPI communicator used to boostrap MDHIM
 * @param filename       the name of the file to open (for now, it is only the mdhim configuration)
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Open(hxhim_t *hx, const MPI_Comm bootstrap_comm) {
    if (!hx) {
        return HXHIM_ERROR;
    }

    if (!(hx->p = new hxhim_private_t())) {
        return HXHIM_ERROR;
    }

    hx->p->running = true;
    hx->p->results = new hxhim::Return(HXHIM_NOP, nullptr);

    hx->p->thread = std::thread(backgroundPUT, hx);

    return hxhim_default_config_reader(hx, bootstrap_comm);
}

/**
 * hxhimOpen
 * Start a HXHIM session
 *
 * @param hx             the HXHIM session
 * @param bootstrap_comm the MPI communicator used to boostrap MDHIM
 * @param filename       the name of the file to open (for now, it is only the mdhim configuration)
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimOpen(hxhim_t *hx, const MPI_Comm bootstrap_comm) {
    return hxhim::Open(hx, bootstrap_comm);
}

/**
 * Close
 * Terminates a HXHIM session
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Close(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    hx->p->running = false;
    hx->p->puts.start_processing.notify_all();
    hx->p->puts.done_processing.notify_all();

    // clear out unflushed work in the work queue
    hxhim::clean(hx->p->puts.head);
    hxhim::clean(hx->p->gets.head);
    hxhim::clean(hx->p->getops.head);
    hxhim::clean(hx->p->deletes.head);
    hxhim::clean(hx->p->unsafe_puts.head);
    hxhim::clean(hx->p->unsafe_gets.head);
    hxhim::clean(hx->p->unsafe_getops.head);
    hxhim::clean(hx->p->unsafe_deletes.head);

    {
        std::unique_lock<std::mutex>(hx->p->results_mutex);
        delete hx->p->results;
        hx->p->results = nullptr;
    }

    // clean up mdhim
    if (hx->p->md) {
        mdhim::Close(hx->p->md);
        delete hx->p->md;
        hx->p->md = nullptr;
    }

    // clean up the mdhim options
    if (hx->p->mdhim_opts) {
        mdhim_options_destroy(hx->p->mdhim_opts);
        delete hx->p->mdhim_opts;
        hx->p->mdhim_opts = nullptr;
    }

    hx->p->thread.join();

    // clean up pointer to private data
    delete hx->p;
    hx->p = nullptr;

    return HXHIM_SUCCESS;
}

/**
 * hxhimClose
 * Terminates a HXHIM session
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimClose(hxhim_t *hx) {
    return hxhim::Close(hx);
}

/**
 * Commit
 * Commits all flushed data to disk
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Commit(hxhim_t *hx) {
    if (!hx || !hx->p || !hx->p->md) {
        return HXHIM_ERROR;
    }

    return (mdhim::Commit(hx->p->md, nullptr) == MDHIM_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * hxhimCommit
 * Commits all flushed data to disk
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimCommit(hxhim_t *hx) {
    return hxhim::Commit(hx);
}

/**
 * StatFlush
 * Flushes the MDHIM statistics
 * Mainly needed for BGetOp
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::StatFlush(hxhim_t *hx) {
    if (!hx || !hx->p || !hx->p->md) {
        return HXHIM_ERROR;
    }

    return (mdhim::StatFlush(hx->p->md, nullptr) == MDHIM_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * hxhimStatFlush
 * Flushes the MDHIM statistics
 * Mainly needed for BGetOp
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimStatFlush(hxhim_t *hx) {
    return hxhim::StatFlush(hx);
}

/**
 * FlushAllPuts
 * Flushes all queued safe and unsafe PUTs
 * The internal queues are cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::FlushAllPuts(hxhim_t *hx) {
    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *curr = &head;

    combine_results(curr, FlushPuts(hx));
    combine_results(curr, Unsafe::FlushPuts(hx));

    return hxhim::return_results(head);
}

/**
 * FlushAllPuts
 * Flushes all queued safe and unsafe PUTs
 * The internal queues are cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushAllPuts(hxhim_t *hx) {
    return hxhim_return_init(hxhim::FlushAllPuts(hx));
}

/**
 * FlushAllGets
 * Flushes all queued safe and unsafe GETs
 * The internal queues are cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::FlushAllGets(hxhim_t *hx) {
    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *curr = &head;

    combine_results(curr, FlushGets(hx));
    combine_results(curr, Unsafe::FlushGets(hx));

    return hxhim::return_results(head);
}

/**
 * hxhimFlushAllGets
 * Flushes all queued safe and unsafe GETs
 * The internal queues are cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushAllGets(hxhim_t *hx) {
    return hxhim_return_init(hxhim::FlushAllGets(hx));
}

/**
 * FlushAllGetOps
 * Flushes all queued safe and unsafe GETs
 * The internal queues are cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::FlushAllGetOps(hxhim_t *hx) {
    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *curr = &head;

    combine_results(curr, FlushGetOps(hx));
    combine_results(curr, Unsafe::FlushGetOps(hx));

    return hxhim::return_results(head);
}

/**
 * hxhimFlushAllGetOps
 * Flushes all queued safe and unsafe GETs
 * The internal queues are cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushAllGetOps(hxhim_t *hx) {
    return hxhim_return_init(hxhim::FlushAllGetOps(hx));
}

/**
 * FlushAllDeletes
 * Flushes all queued safe and unsafe DELs
 * The internal queues are cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::FlushAllDeletes(hxhim_t *hx) {
    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *curr = &head;

    combine_results(curr, FlushDeletes(hx));
    combine_results(curr, Unsafe::FlushDeletes(hx));

    return hxhim::return_results(head);
}

/**
 * hxhimFlushAllDeletes
 * Flushes all queued safe and unsafe DELs
 * The internal queues are cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushAllDeletes(hxhim_t *hx) {
    return hxhim_return_init(hxhim::FlushAllDeletes(hx));
}

/**
 * FlushAll
 * Flushes all queued safe and unsafe work
 * The internal queues are cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::FlushAll(hxhim_t *hx) {
    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *curr = &head;

    combine_results(curr, FlushPuts(hx));
    combine_results(curr, Unsafe::FlushPuts(hx));
    combine_results(curr, FlushGets(hx));
    combine_results(curr, Unsafe::FlushGets(hx));
    combine_results(curr, FlushGetOps(hx));
    combine_results(curr, Unsafe::FlushGetOps(hx));
    combine_results(curr, FlushDeletes(hx));
    combine_results(curr, Unsafe::FlushDeletes(hx));

    return hxhim::return_results(head);
}

/**
 * hxhimFlushAll
 * Flushes all queued safe and unsafe work
 * The internal queues are cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushAll(hxhim_t *hx) {
    return hxhim_return_init(hxhim::FlushAll(hx));
}

/**
 * FlushPuts
 * Flushes all queued PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::FlushPuts(hxhim_t *hx) {
    hxhim::Unsent<hxhim::PutData> &unsent = hx->p->puts;
    std::unique_lock<std::mutex> lock(unsent.mutex);
    unsent.force = true;
    unsent.start_processing.notify_all();

    // wait for flush to complete
    while (hx->p->running && unsent.force) {
        unsent.done_processing.wait(lock, [&](){ return !hx->p->running || !unsent.force; });
    }

    // retrieve all results
    std::unique_lock<std::mutex> results_lock(hx->p->results_mutex);
    hxhim::Return *ret = hxhim::return_results(*hx->p->results);
    hx->p->results->Next(nullptr);

    return ret;
}

/**
 * hxhimFlushPuts
 * Flushes all queued PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushPuts(hxhim_t *hx) {
    return hxhim_return_init(hxhim::FlushPuts(hx));
}

/**
 * FlushGets
 * Flushes all queued GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::FlushGets(hxhim_t *hx) {
    hxhim::Unsent<hxhim::GetData> &gets = hx->p->gets;
    std::lock_guard<std::mutex> lock(gets.mutex);

    hxhim::GetData *curr = gets.head;
    if (!curr) {
        return HXHIM_SUCCESS;
    }

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;

    void **keys = nullptr;
    std::size_t *key_lens = nullptr;

    // write complete batches
    while (curr->next) {
        keys = new void *[HXHIM_MAX_BULK_GET_OPS]();
        key_lens = new std::size_t[HXHIM_MAX_BULK_GET_OPS]();

        for(std::size_t i = 0; i < HXHIM_MAX_BULK_GET_OPS; i++) {
            sp_to_key(curr->subjects[i], curr->subject_lens[i], curr->predicates[i], curr->predicate_lens[i], &keys[i], &key_lens[i]);
        }

        // GET the batch
        res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::BGet(hx->p->md, nullptr, keys, key_lens, HXHIM_MAX_BULK_GET_OPS, TransportGetMessageOp::GET_EQ)));

        // cleanup
        for(std::size_t i = 0; i < HXHIM_MAX_BULK_GET_OPS; i++) {
            ::operator delete(keys[i]);
        }
        delete [] keys;
        delete [] key_lens;

        // go to the next batch
        hxhim::GetData *next = curr->next;
        delete curr;
        curr = next;
    }

    // write final (likely incomplete) batch
    keys = new void *[gets.last_count]();
    key_lens = new std::size_t[gets.last_count]();

    for(std::size_t i = 0; i < gets.last_count; i++) {
        sp_to_key(curr->subjects[i], curr->subject_lens[i], curr->predicates[i], curr->predicate_lens[i], &keys[i], &key_lens[i]);
    }

    // GET the batch
    res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::BGet(hx->p->md, nullptr, keys, key_lens, gets.last_count, TransportGetMessageOp::GET_EQ)));

    // cleanup
    for(std::size_t i = 0; i < gets.last_count; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;
    delete [] key_lens;

    // delete the last batch
    delete curr;

    gets.head = gets.tail = nullptr;

    return hxhim::return_results(head);
}

/**
 * hxhimFlushGets
 * Flushes all queued GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushGets(hxhim_t *hx) {
    return hxhim_return_init(hxhim::FlushGets(hx));
}

/**
 * FlushGetOps
 * Flushes all queued GETs with specific operations
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::FlushGetOps(hxhim_t *hx) {
    hxhim::Unsent<hxhim::GetOpData> &getops = hx->p->getops;
    std::lock_guard<std::mutex> lock(getops.mutex);

    hxhim::GetOpData *curr = getops.head;
    if (!curr) {
        return HXHIM_SUCCESS;
    }

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;

    // write complete batches
    while (curr->next) {
        for(std::size_t i = 0; i < HXHIM_MAX_BULK_GET_OPS; i++) {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(curr->subjects[i], curr->subject_lens[i], curr->predicates[i], curr->predicate_lens[i], &key, &key_len);

            // GETOP the key
            res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::BGetOp(hx->p->md, nullptr, key, key_len, HXHIM_MAX_BULK_GET_OPS, curr->ops[i])));
            ::operator delete(key);
        }

        // go to the next batch
        hxhim::GetOpData *next = curr->next;
        delete curr;
        curr = next;
    }

    // write final (likely incomplete) batch
    for(std::size_t i = 0; i < getops.last_count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(curr->subjects[i], curr->subject_lens[i], curr->predicates[i], curr->predicate_lens[i], &key, &key_len);

        // GETOP the key
        res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::BGetOp(hx->p->md, nullptr, key, key_len, getops.last_count, curr->ops[i])));
        ::operator delete(key);
    }

    // delete the last batch
    delete curr;

    getops.head = getops.tail = nullptr;

    return hxhim::return_results(head);
}

/**
 * hxhimFlushGetOps
 * Flushes all queued GETs with specific operations
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushGetOps(hxhim_t *hx) {
    return hxhim_return_init(hxhim::FlushGetOps(hx));
}

/**
 * FlushDeletes
 * Flushes all queued DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::FlushDeletes(hxhim_t *hx) {
    hxhim::Unsent<hxhim::DeleteData> &dels = hx->p->deletes;
    std::lock_guard<std::mutex> lock(dels.mutex);

    hxhim::DeleteData *curr = dels.head;
    if (!curr) {
        return HXHIM_SUCCESS;
    }

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;

    void **keys = nullptr;
    std::size_t *key_lens = nullptr;

    // write complete batches
    while (curr->next) {
        keys = new void *[HXHIM_MAX_BULK_DEL_OPS]();
        key_lens = new std::size_t[HXHIM_MAX_BULK_DEL_OPS]();

        for(std::size_t i = 0; i < HXHIM_MAX_BULK_DEL_OPS; i++) {
            sp_to_key(curr->subjects[i], curr->subject_lens[i], curr->predicates[i], curr->predicate_lens[i], &keys[i], &key_lens[i]);
        }

        // DEL the batch
        res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_DEL, mdhim::BDelete(hx->p->md, nullptr, keys, key_lens, HXHIM_MAX_BULK_DEL_OPS)));

        // cleanup
        for(std::size_t i = 0; i < HXHIM_MAX_BULK_DEL_OPS; i++) {
            ::operator delete(keys[i]);
        }
        delete [] keys;
        delete [] key_lens;

        // go to the next batch
        hxhim::DeleteData *next = curr->next;
        delete curr;
        curr = next;
    }

    // write final (likely incomplete) batch
    keys = new void *[dels.last_count]();
    key_lens = new std::size_t[dels.last_count]();

    for(std::size_t i = 0; i < dels.last_count; i++) {
        sp_to_key(curr->subjects[i], curr->subject_lens[i], curr->predicates[i], curr->predicate_lens[i], &keys[i], &key_lens[i]);
    }

    // DEL the batch
    res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_DEL, mdhim::BDelete(hx->p->md, nullptr, keys, key_lens, dels.last_count)));

    // cleanup
    for(std::size_t i = 0; i < dels.last_count; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;
    delete [] key_lens;

    // delete the last batch
    delete curr;

    dels.head = dels.tail = nullptr;

    return hxhim::return_results(head);
}

/**
 * hxhimFlushDeletes
 * Flushes all queued DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushDeletes(hxhim_t *hx) {
    return hxhim_return_init(hxhim::FlushDeletes(hx));
}

/**
 * Flush
 *     1. Do all PUTs
 *     2. Do all GETs
 *     3. Do all GET_OPs
 *     4. Do all DELs
 *
 * @param hx
 * @return An array of results (3 values)
 */
hxhim::Return *hxhim::Flush(hxhim_t *hx) {
    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *curr = &head;

    combine_results(curr, FlushPuts(hx));
    combine_results(curr, FlushGets(hx));
    combine_results(curr, FlushGetOps(hx));
    combine_results(curr, FlushDeletes(hx));

    return hxhim::return_results(head);
}

/**
 * hxhimFlush
 * Push all queued work into MDHIM
 *
 * @param hx
 * @return An array of results (3 values)
 */
hxhim_return_t *hxhimFlush(hxhim_t *hx) {
    return hxhim_return_init(hxhim::Flush(hx));
}

/**
 * Put
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the object to put
 * @param object_len   the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Put(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len,
               void *object, std::size_t object_len) {
    if (!hx        || !hx->p         ||
        !subject   || !subject_len   ||
        !predicate || !predicate_len ||
        !object    || !object_len)    {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::PutData> &puts = hx->p->puts;
    std::lock_guard<std::mutex> lock(puts.mutex);

    // no previous batch
    if (!puts.tail) {
        puts.head       = new hxhim::PutData();
        puts.tail       = puts.head;
        puts.last_count = 0;
    }

    // filled the current batch
    if (puts.last_count == HXHIM_MAX_BULK_PUT_OPS) {
        hxhim::PutData *next = new hxhim::PutData();
        next->prev      = puts.tail;
        puts.tail->next = next;
        puts.tail       = next;
        puts.last_count = 0;
        puts.full_batches++;
        puts.start_processing.notify_one();
    }

    std::size_t &i = puts.last_count;
    puts.tail->subjects[i] = subject;
    puts.tail->subject_lens[i] = subject_len;
    puts.tail->predicates[i] = predicate;
    puts.tail->predicate_lens[i] = predicate_len;
    puts.tail->objects[i] = object;
    puts.tail->object_lens[i] = object_len;
    i++;

    return HXHIM_SUCCESS;
}

/**
 * hxhimPut
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the object to put
 * @param object_len   the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimPut(hxhim_t *hx,
             void *subject, std::size_t subject_len,
             void *predicate, std::size_t predicate_len,
             void *object, std::size_t object_len) {
    return hxhim::Put(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      object, object_len);
}

/**
 * Get
 * Add a GET into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to get
 * @param subject_len  the length of the subject to get
 * @param prediate     the prediate to get
 * @param prediate_len the length of the prediate to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Get(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len) {
    if (!hx        || !hx->p         ||
        !subject   || !subject_len   ||
        !predicate || !predicate_len) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::GetData> &gets = hx->p->gets;
    std::lock_guard<std::mutex> lock(gets.mutex);

    // no previous batch
    if (!gets.tail) {
        gets.head       = new hxhim::GetData();
        gets.tail       = gets.head;
        gets.last_count = 0;
    }

    // filled the current batch
    if (gets.last_count == HXHIM_MAX_BULK_GET_OPS) {
        gets.tail->next = new hxhim::GetData();
        gets.tail       = gets.tail->next;
        gets.last_count = 0;
        gets.full_batches++;
    }

    std::size_t &i = gets.last_count;
    gets.tail->subjects[i] = subject;
    gets.tail->subject_lens[i] = subject_len;
    gets.tail->predicates[i] = predicate;
    gets.tail->predicate_lens[i] = predicate_len;
    i++;
    return HXHIM_SUCCESS;
}

/**
 * hxhimGet
 * Add a GET into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to get
 * @param subject_len  the length of the subject to get
 * @param prediate     the prediate to get
 * @param prediate_len the length of the prediate to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGet(hxhim_t *hx,
             void *subject, std::size_t subject_len,
             void *predicate, std::size_t predicate_len) {
    return hxhim::Get(hx,
                      subject, subject_len,
                      predicate, predicate_len);
}

/**
 * Delete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Delete(hxhim_t *hx,
                  void *subject, std::size_t subject_len,
                  void *predicate, std::size_t predicate_len) {
    if (!hx        || !hx->p         ||
        !subject   || !subject_len   ||
        !predicate || !predicate_len) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::DeleteData> &dels = hx->p->deletes;
    std::lock_guard<std::mutex> lock(dels.mutex);

    // no previous batch
    if (!dels.tail) {
        dels.head       = new hxhim::DeleteData();
        dels.tail       = dels.head;
        dels.last_count = 0;
    }

    // filled the current batch
    if (dels.last_count == HXHIM_MAX_BULK_DEL_OPS) {
        dels.tail->next = new hxhim::DeleteData();
        dels.tail       = dels.tail->next;
        dels.last_count = 0;
        dels.full_batches++;
    }

    std::size_t &i = dels.last_count;
    dels.tail->subjects[i] = subject;
    dels.tail->subject_lens[i] = subject_len;
    dels.tail->predicates[i] = predicate;
    dels.tail->predicate_lens[i] = predicate_len;
    i++;

    return HXHIM_SUCCESS;
}

/**
 * hxhimDelete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @param object       the object associated with the key
 * @param object_len   the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimDelete(hxhim_t *hx,
                void *subject, std::size_t subject_len,
                void *predicate, std::size_t predicate_len) {
    return hxhim::Delete(hx,
                         subject, subject_len,
                         predicate, predicate_len);
}

/**
 * BPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BPut(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                void **objects, std::size_t *object_lens,
                std::size_t count) {
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !objects    || !object_lens) {
        return HXHIM_ERROR;
    }

    if (count) {
        hxhim::Unsent<hxhim::PutData> &puts = hx->p->puts;
        std::lock_guard<std::mutex> lock(puts.mutex);

        // no previous batch
        if (!puts.tail) {
            puts.head       = new hxhim::PutData();
            puts.tail       = puts.head;
            puts.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (puts.last_count == HXHIM_MAX_BULK_PUT_OPS) {
                hxhim::PutData *next = new hxhim::PutData();
                next->prev      = puts.tail;
                puts.tail->next = next;
                puts.tail       = next;
                puts.last_count = 0;
                puts.full_batches++;
                puts.start_processing.notify_one();
            }

            std::size_t &i = puts.last_count;
            puts.tail->subjects[i] = subjects[c];
            puts.tail->subject_lens[i] = subject_lens[c];
            puts.tail->predicates[i] = predicates[c];
            puts.tail->predicate_lens[i] = predicate_lens[c];
            puts.tail->objects[i] = objects[c];
            puts.tail->object_lens[i] = object_lens[c];
            i++;
        }
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBPut(hxhim_t *hx,
              void **subjects, std::size_t *subject_lens,
              void **predicates, std::size_t *predicate_lens,
              void **objects, std::size_t *object_lens,
              std::size_t count) {
    return hxhim::BPut(hx,
                       subjects, subject_lens,
                       predicates, predicate_lens,
                       objects, object_lens,
                       count);
}

/**
 * BGet
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGet(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                std::size_t count) {
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens) {
        return HXHIM_ERROR;
    }

    if (count) {
        hxhim::Unsent<hxhim::GetData> &gets = hx->p->gets;
        std::lock_guard<std::mutex> lock(gets.mutex);

        // no previous batch
        if (!gets.tail) {
            gets.head       = new hxhim::GetData();
            gets.tail       = gets.head;
            gets.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (gets.last_count == HXHIM_MAX_BULK_GET_OPS) {
                gets.tail->next = new hxhim::GetData();
                gets.tail       = gets.tail->next;
                gets.last_count = 0;
                gets.full_batches++;
            }

            std::size_t &i = gets.last_count;
            gets.tail->subjects[i] = subjects[c];
            gets.tail->subject_lens[i] = subject_lens[c];
            gets.tail->predicates[i] = predicates[c];
            gets.tail->predicate_lens[i] = predicate_lens[c];
            i++;
        }
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBGet
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGet(hxhim_t *hx,
              void **subjects, std::size_t *subject_lens,
              void **predicates, std::size_t *predicate_lens,
              std::size_t count) {
    return hxhim::BGet(hx,
                       subjects, subject_lens,
                       predicates, predicate_lens,
                       count);
}

/**
 * BGetOp
 * Add a BGET into the work queue
 *
 * @param hx         the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGetOp(hxhim_t *hx,
                  void *subject, std::size_t subject_len,
                  void *predicate, std::size_t predicate_len,
                  std::size_t num_records, enum TransportGetMessageOp op) {
    if (!hx        || !hx->p         ||
        !subject   || !subject_len   ||
        !predicate || !predicate_len) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::GetOpData> &getops = hx->p->getops;
    std::lock_guard<std::mutex> lock(getops.mutex);

    // no previous batch
    if (!getops.tail) {
        getops.head       = new hxhim::GetOpData();
        getops.tail       = getops.head;
        getops.last_count = 0;
    }

    // filled the current batch
    if (getops.last_count == HXHIM_MAX_BULK_GET_OPS) {
        getops.tail->next = new hxhim::GetOpData();
        getops.tail       = getops.tail->next;
        getops.last_count = 0;
        getops.full_batches++;
    }

    std::size_t &i = getops.last_count;
    getops.tail->subjects[i] = subject;
    getops.tail->subject_lens[i] = subject_len;
    getops.tail->predicates[i] = predicate;
    getops.tail->predicate_lens[i] = predicate_len;
    getops.tail->ops[i] = op;
    i++;

    return HXHIM_SUCCESS;
}

/**
 * hxhimBGetOp
 * Add a BGET into the work queue
 *
 * @param hx         the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGetOp(hxhim_t *hx,
                void *subject, std::size_t subject_len,
                void *predicate, std::size_t predicate_len,
                std::size_t num_records, enum TransportGetMessageOp op) {
    return hxhim::BGetOp(hx,
                         subject, subject_len,
                         predicate, predicate_len,
                         num_records, op);
}

/**
 * BDelete
 * Add a BDEL into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to delete
 * @param subject_lens  the lengths of the subjects to delete
 * @param prediates     the prediates to delete
 * @param prediate_lens the lengths of the prediates to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BDelete(hxhim_t *hx,
                   void **subjects, std::size_t *subject_lens,
                   void **predicates, std::size_t *predicate_lens,
                   std::size_t count) {
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens) {
        return HXHIM_ERROR;
    }

    if (count) {
        hxhim::Unsent<hxhim::DeleteData> &dels = hx->p->deletes;
        std::lock_guard<std::mutex> lock(dels.mutex);

        // no previous batch
        if (!dels.tail) {
            dels.head     = new hxhim::DeleteData();
            dels.tail     = dels.head;
            dels.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (dels.last_count == HXHIM_MAX_BULK_DEL_OPS) {
                dels.tail->next = new hxhim::DeleteData();
                dels.tail       = dels.tail->next;
                dels.last_count = 0;
                dels.full_batches++;
            }

            std::size_t &i = dels.last_count;
            dels.tail->subjects[i] = subjects[c];
            dels.tail->subject_lens[i] = subject_lens[c];
            dels.tail->predicates[i] = predicates[c];
            dels.tail->predicate_lens[i] = predicate_lens[c];
            i++;
        }
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBDelete
 * Add a BDEL into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to delete
 * @param subject_lens  the lengths of the subjects to delete
 * @param prediates     the prediates to delete
 * @param prediate_lens the lengths of the prediates to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBDelete(hxhim_t *hx,
                 void **subjects, std::size_t *subject_lens,
                 void **predicates, std::size_t *predicate_lens,
                 std::size_t count) {
    return hxhim::BDelete(hx,
                          subjects, subject_lens,
                          predicates, predicate_lens,
                          count);
}

/**
 * GetStats
 * Collective operation
 * Each desired pointer should be preallocated with space for md->size values
 *
 * @param hx             the HXHIM session
 * @param rank           the rank that is collecting the data
 * @param get_put_times  whether or not to get put_times
 * @param put_times      the array of put times from each rank
 * @param get_num_puts   whether or not to get num_puts
 * @param num_puts       the array of number of puts from each rank
 * @param get_get_times  whether or not to get get_times
 * @param get_times      the array of get times from each rank
 * @param get_num_gets   whether or not to get num_gets
 * @param num_gets       the array of number of gets from each rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetStats(hxhim_t *hx, const int rank,
                    const bool get_put_times, long double *put_times,
                    const bool get_num_puts, std::size_t *num_puts,
                    const bool get_get_times, long double *get_times,
                    const bool get_num_gets, std::size_t *num_gets) {
    return mdhim::GetStats(hx->p->md, rank,
                           get_put_times, put_times,
                           get_num_puts, num_puts,
                           get_get_times, get_times,
                           get_num_gets, num_gets);
}

/**
 * hxhimGetStats
 * Collective operation
 * Each desired pointer should be preallocated with space for md->size values
 *
 * @param hx             the HXHIM session
 * @param rank           the rank that is collecting the data
 * @param get_put_times  whether or not to get put_times
 * @param put_times      the array of put times from each rank
 * @param get_num_puts   whether or not to get num_puts
 * @param num_puts       the array of number of puts from each rank
 * @param get_get_times  whether or not to get get_times
 * @param get_times      the array of get times from each rank
 * @param get_num_gets   whether or not to get num_gets
 * @param num_gets       the array of number of gets from each rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetStats(hxhim_t *hx, const int rank,
                  const int get_put_times, long double *put_times,
                  const int get_num_puts, std::size_t *num_puts,
                  const int get_get_times, long double *get_times,
                  const int get_num_gets, std::size_t *num_gets) {
    return hxhim::GetStats(hx, rank,
                           get_put_times, put_times,
                           get_num_puts, num_puts,
                           get_get_times, get_times,
                           get_num_gets, num_gets);
}
