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

    // clear out unsent work in the work queue
    hx->p->puts.data.clear();
    hx->p->gets.data.clear();
    hx->p->getops.data.clear();
    hx->p->dels.data.clear();
    hx->p->unsafe_puts.data.clear();
    hx->p->unsafe_gets.data.clear();
    hx->p->unsafe_getops.data.clear();
    hx->p->unsafe_dels.data.clear();

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
 * FlushAllGets
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
 * FlushAllGetOps
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
 * FlushAllDeletes
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
 * FlushAll
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
    std::lock_guard<std::mutex> lock(hx->p->puts.mutex);
    std::list<hxhim::spo_t> &puts = hx->p->puts.data;

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;

    static const std::size_t multiplier = 4;
    while (puts.size()) {
        // Generate up to 1 MAX_BULK_OPS worth of data
        const std::size_t count = std::min(puts.size(), (std::size_t) HXHIM_MAX_BULK_PUT_OPS);
        const std::size_t total = multiplier * count;

        void **keys = new void *[total]();
        std::size_t *key_lens = new std::size_t[total]();
        void **values = new void *[total]();
        std::size_t *value_lens = new std::size_t[total]();

        if (keys   && key_lens   &&
            values && value_lens) {
            std::size_t offset = 0;
            for(std::size_t i = 0; i < count; i++) {
                // generate the 6 different permutations of the subject-predicate-object triple
                const hxhim::spo_t &put = puts.front();

                sp_to_key(put.subject,   put.subject_len,   put.predicate, put.predicate_len, &keys[offset], &key_lens[offset]);
                values[offset] = put.object;
                value_lens[offset] = put.object_len;
                offset++;

                sp_to_key(put.subject,   put.subject_len,   put.object,    put.object_len,    &keys[offset], &key_lens[offset]);
                values[offset] = put.predicate;
                value_lens[offset] = put.predicate_len;
                offset++;

                sp_to_key(put.predicate, put.predicate_len, put.subject,   put.subject_len,   &keys[offset], &key_lens[offset]);
                values[offset] = put.object;
                value_lens[offset] = put.object_len;
                offset++;

                sp_to_key(put.predicate, put.predicate_len, put.object,    put.object_len,    &keys[offset], &key_lens[offset]);
                values[offset] = put.subject;
                value_lens[offset] = put.subject_len;
                offset++;

                // sp_to_key(put.object,    put.object_len,    put.subject,   put.subject_len,   &keys[offset], &key_lens[offset]);
                // values[offset] = put.predicate;
                // value_lens[offset] = put.predicate_len;
                // offset++;

                // sp_to_key(put.object,    put.object_len,    put.predicate, put.predicate_len, &keys[offset], &key_lens[offset]);
                // values[offset] = put.subject;
                // value_lens[offset] = put.subject_len;
                // offset++;

                puts.pop_front();
            }

            res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_PUT, mdhim::BPut(hx->p->md, nullptr, keys, key_lens, values, value_lens, total)));

            for(std::size_t i = 0; i < total; i++) {
                ::operator delete(keys[i]);
            }
        }
        else {
            for(std::size_t i = 0; i < count; i++) {
                puts.pop_front();
            }
        }

        // cleanup
        delete [] keys;
        delete [] key_lens;
        delete [] values;
        delete [] value_lens;
    }

    return hxhim::return_results(head);
}

/**
 * FlushPuts
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
    std::lock_guard<std::mutex> lock(hx->p->gets.mutex);
    std::list<hxhim::sp_t> &gets = hx->p->gets.data;

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;

    while (gets.size()) {
        // Process up to 1 MAX_BULK_OPS worth of data
        std::size_t count = std::min(gets.size(), (std::size_t) HXHIM_MAX_BULK_GET_OPS);

        // copy the keys and lengths into arrays
        void **keys = new void *[count]();
        std::size_t *key_lens = new std::size_t[count]();

        if (keys && key_lens) {
            for(std::size_t i = 0; i < count; i++) {
                // convert current subject+predicate into a key
                void *key = nullptr;
                std::size_t key_len = 0;

                sp_to_key(gets.front().subject, gets.front().subject_len, gets.front().predicate, gets.front().predicate_len, &key, &key_len);

                // move the constructed key into the buffer
                keys    [i] = key;
                key_lens[i] = key_len;

                gets.pop_front();
            }

            TransportBGetRecvMessage *bgrm = mdhim::BGet(hx->p->md, nullptr, keys, key_lens, count, TransportGetMessageOp::GET_EQ);
            bgrm->clean = true;
            res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_PUT, bgrm));
        }
        else {
            for(std::size_t i = 0; i < count; i++) {
                gets.pop_front();
            }
        }

        // cleanup
        for(std::size_t i = 0; i < count; i++) {
            ::operator delete(keys[i]);
        }
        delete [] keys;
        delete [] key_lens;
    }

    return hxhim::return_results(head);
}

/**
 * FlushGets
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
    std::lock_guard<std::mutex> lock(hx->p->getops.mutex);
    std::list<hxhim::sp_op_t> &gets = hx->p->getops.data;

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;

    while (gets.size()) {
        // convert current subject+predicate into a key
        void *key = nullptr;
        std::size_t key_len = 0;

        sp_to_key(gets.front().subject, gets.front().subject_len, gets.front().predicate, gets.front().predicate_len, &key, &key_len);

        res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::BGetOp(hx->p->md, nullptr, key, key_len, gets.front().num_records, gets.front().op)));

        // cleanup
        ::operator delete(key);
        gets.pop_front();
    }

    return hxhim::return_results(head);
}

/**
 * FlushGetOps
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
    std::lock_guard<std::mutex> lock(hx->p->dels.mutex);
    std::list<hxhim::sp_t> &dels = hx->p->dels.data;

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;

    while (dels.size()) {
        std::size_t count = std::min(dels.size(), (std::size_t) HXHIM_MAX_BULK_DEL_OPS);

        // copy the keys and lengths into arrays
        void **keys = new void *[count]();
        std::size_t *key_lens = new std::size_t[count]();

        if (keys && key_lens) {
            for(std::size_t i = 0; i < count; i++) {
                // convert current subject+predicate into a key
                void *key = nullptr;
                std::size_t key_len = 0;

                sp_to_key(dels.front().subject, dels.front().subject_len, dels.front().predicate, dels.front().predicate_len, &key, &key_len);

                // move the constructed key into the buffer
                keys    [i] = key;
                key_lens[i] = key_len;

                dels.pop_front();
            }

            res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_DEL, mdhim::BDelete(hx->p->md, nullptr, keys, key_lens, count)));

            for(std::size_t i = 0; i < count; i++) {
                ::operator delete(keys[i]);
            }
        }
        else {
            for(std::size_t i = 0; i < count; i++) {
                dels.pop_front();
            }
        }

        // cleanup
        delete [] keys;
        delete [] key_lens;
    }

    return hxhim::return_results(head);
}

/**
 * FlushDeletes
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

    for(curr = curr->Next(FlushPuts(hx));    curr->Next(); curr = curr->Next());
    for(curr = curr->Next(FlushGets(hx));    curr->Next(); curr = curr->Next());
    for(curr = curr->Next(FlushGetOps(hx));  curr->Next(); curr = curr->Next());
    for(curr = curr->Next(FlushDeletes(hx)); curr->Next(); curr = curr->Next());

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

    hxhim::spo_t spo;
    spo.subject = subject;
    spo.subject_len = subject_len;
    spo.predicate = predicate;
    spo.predicate_len = predicate_len;
    spo.object = object;
    spo.object_len = object_len;

    std::lock_guard<std::mutex> lock(hx->p->puts.mutex);
    hx->p->puts.data.emplace_back(spo);

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

    hxhim::sp_t sp;
    sp.subject = subject;
    sp.subject_len = subject_len;
    sp.predicate = predicate;
    sp.predicate_len = predicate_len;

    std::lock_guard<std::mutex> lock(hx->p->gets.mutex);
    hx->p->gets.data.emplace_back(sp);
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

    hxhim::sp_t sp;
    sp.subject = subject;
    sp.subject_len = subject_len;
    sp.predicate = predicate;
    sp.predicate_len = predicate_len;

    std::lock_guard<std::mutex> lock(hx->p->dels.mutex);
    hx->p->dels.data.emplace_back(sp);
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

    std::lock_guard<std::mutex> lock(hx->p->puts.mutex);
    for(std::size_t i = 0; i < count; i++) {
        hxhim::spo_t spo;
        spo.subject = subjects[i];
        spo.subject_len = subject_lens[i];
        spo.predicate = predicates[i];
        spo.predicate_len = predicate_lens[i];
        spo.object = objects[i];
        spo.object_len = object_lens[i];
        hx->p->puts.data.emplace_back(spo);
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
 * @return A wrapped return value containing responses
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

    std::lock_guard<std::mutex> lock(hx->p->gets.mutex);
    for(std::size_t i = 0; i < count; i++) {
        hxhim::sp_t sp;
        sp.subject = subjects[i];
        sp.subject_len = subject_lens[i];
        sp.predicate = predicates[i];
        sp.predicate_len = predicate_lens[i];
        hx->p->gets.data.emplace_back(sp);
    }

    return MDHIM_SUCCESS;
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
              void **subjects, size_t *subject_lens,
              void **predicates, size_t *predicate_lens,
              size_t count) {
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
 * @param keys       the keys to bget
 * @param key_lens   the length of the keys to bget
 * @return A wrapped return value containing responses
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

    hxhim::sp_op_t sp;
    sp.subject = subject;
    sp.subject_len = subject_len;
    sp.predicate = predicate;
    sp.predicate_len = predicate_len;
    sp.num_records = num_records;
    sp.op = op;

    std::lock_guard<std::mutex> lock(hx->p->getops.mutex);
    hx->p->getops.data.emplace_back(sp);

    return MDHIM_SUCCESS;
}

/**
 * hxhimBGetOp
 * Add a BGET into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bget
 * @param key_lens   the length of the keys to bget
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

    std::lock_guard<std::mutex> lock(hx->p->dels.mutex);
    for(std::size_t i = 0; i < count; i++) {
        hxhim::sp_t sp;
        sp.subject = subjects[i];
        sp.subject_len = subject_lens[i];
        sp.predicate = predicates[i];
        sp.predicate_len = predicate_lens[i];
        hx->p->dels.data.emplace_back(sp);
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
                 void **subjects, size_t *subject_lens,
                 void **predicates, size_t *predicate_lens,
                 size_t count) {
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
 * @return MDHIM_SUCCESS or MDHIM_ERROR
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
 * mdhimGetStats
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
 * @return MDHIM_SUCCESS or MDHIM_ERROR
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
