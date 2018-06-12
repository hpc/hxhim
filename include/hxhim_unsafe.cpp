#include <cmath>
#include "mlog2.h"
#include "mlogfacs2.h"

#include "hxhim.h"
#include "hxhim.hpp"
#include "hxhim_private.hpp"
#include "return_private.hpp"
#include "triplestore.hpp"

/**
 * FlushPuts
 * Flushes all queued unsafe PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::Unsafe::FlushPuts(hxhim_t *hx) {
    std::lock_guard<std::mutex> lock(hx->p->unsafe_puts.mutex);
    std::list<hxhim::unsafe_spo_t> &puts = hx->p->unsafe_puts.data;

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;

    while (puts.size()) {
        // Generate up to 1 MAX_BULK_OPS worth of data
        std::size_t count = std::min(puts.size(), (std::size_t) HXHIM_MAX_BULK_PUT_OPS);
        std::size_t hexcount = 6 * count;

        // copy the keys and lengths into arrays
        void **keys = new void *[hexcount]();
        std::size_t *key_lens = new std::size_t[hexcount]();
        void **values = new void *[hexcount]();
        std::size_t *value_lens = new std::size_t[hexcount]();
        int *databases = new int[hexcount]();

        if (keys   && key_lens   &&
            values && value_lens) {
            for(std::size_t i = 0; i < count; i++) {
                const hxhim::unsafe_spo_t &put = puts.front();
                const std::size_t offset = 6 * i;

                sp_to_key(put.subject,   put.subject_len,   put.predicate, put.predicate_len, &keys[offset + 0], &key_lens[offset + 0]);
                values[offset + 0] = put.object;
                value_lens[offset + 0] = put.object_len;
                databases[offset + 0] = put.database;

                sp_to_key(put.subject,   put.subject_len,   put.object,    put.object_len,    &keys[offset + 1], &key_lens[offset + 1]);
                values[offset + 1] = put.predicate;
                value_lens[offset + 1] = put.predicate_len;
                databases[offset + 1] = put.database;

                sp_to_key(put.predicate, put.predicate_len, put.subject,   put.subject_len,   &keys[offset + 2], &key_lens[offset + 2]);
                values[offset + 2] = put.object;
                value_lens[offset + 2] = put.object_len;
                databases[offset + 2] = put.database;

                sp_to_key(put.predicate, put.predicate_len, put.object,    put.object_len,    &keys[offset + 3], &key_lens[offset + 3]);
                values[offset + 3] = put.subject;
                value_lens[offset + 3] = put.subject_len;
                databases[offset + 3] = put.database;

                sp_to_key(put.object,    put.object_len,    put.subject,   put.subject_len,   &keys[offset + 4], &key_lens[offset + 4]);
                values[offset + 4] = put.predicate;
                value_lens[offset + 4] = put.predicate_len;
                databases[offset + 4] = put.database;

                sp_to_key(put.object,    put.object_len,    put.predicate, put.predicate_len, &keys[offset + 5], &key_lens[offset + 5]);
                values[offset + 5] = put.subject;
                value_lens[offset + 5] = put.subject_len;
                databases[offset + 5] = put.database;

                puts.pop_front();
            }

            res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_PUT, mdhim::Unsafe::BPut(hx->p->md, nullptr, keys, key_lens, values, value_lens, databases, hexcount)));

            for(std::size_t i = 0; i < hexcount; i++) {
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
        delete [] databases;
    }

    return hxhim::return_results(head);
}

/**
 * FlushPuts
 * Flushes all queued unsafe PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimUnsafeFlushPuts(hxhim_t *hx) {
    return hxhim_return_init(hxhim::Unsafe::FlushPuts(hx));
}

/**
 * FlushGets
 * Flushes all queued unsafe GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::Unsafe::FlushGets(hxhim_t *hx) {
    std::lock_guard<std::mutex> lock(hx->p->unsafe_gets.mutex);
    std::list<hxhim::unsafe_sp_t> &gets = hx->p->unsafe_gets.data;

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;
    while (gets.size()) {
        // Process up to 1 MAX_BULK_OPS worth of data
        std::size_t count = std::min(gets.size(), (std::size_t) HXHIM_MAX_BULK_GET_OPS);

        // copy the keys and lengths into arrays
        void **keys = new void *[count]();
        std::size_t *key_lens = new std::size_t[count]();
        int *databases = new int[count]();

        if (keys && key_lens) {
            for(std::size_t i = 0; i < count; i++) {
                // convert current subject+predicate into a key
                void *key = nullptr;
                std::size_t key_len = 0;

                sp_to_key(gets.front().subject, gets.front().subject_len, gets.front().predicate, gets.front().predicate_len, &key, &key_len);

                // move the constructed key into the buffer
                keys     [i] = key;
                key_lens [i] = key_len;
                databases[i] = gets.front().database;

                gets.pop_front();
            }

            TransportBGetRecvMessage *bgrm = mdhim::Unsafe::BGet(hx->p->md, nullptr, keys, key_lens, databases, count, TransportGetMessageOp::GET_EQ);
            bgrm->clean = true;
            res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_PUT, bgrm));
        }
        else {
            for(std::size_t i = 0; i < count; i++) {
                gets.pop_front();
            }
        }

        // cleanup
        delete [] keys;
        delete [] key_lens;
        delete [] databases;
    }

    return hxhim::return_results(head);
}

/**
 * FlushGets
 * Flushes all queued unsafe GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimUnsafeFlushGets(hxhim_t *hx) {
    return hxhim_return_init(hxhim::Unsafe::FlushGets(hx));
}

/**
 * FlushGetOps
 * Flushes all queued GETs with specific operations
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::Unsafe::FlushGetOps(hxhim_t *hx) {
    mdhim::StatFlush(hx->p->md, nullptr);

    std::lock_guard<std::mutex> lock(hx->p->unsafe_getops.mutex);
    std::list<hxhim::unsafe_sp_op_t> &getops = hx->p->unsafe_getops.data;

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;

    // can add some async stuff here
    while (getops.size()) {
        // convert current subject+predicate into a key
        void *key = nullptr;
        std::size_t key_len = 0;

        sp_to_key(getops.front().subject, getops.front().subject_len, getops.front().predicate, getops.front().predicate_len, &key, &key_len);

        TransportBGetRecvMessage *bgrm = mdhim::Unsafe::BGetOp(hx->p->md, nullptr, key, key_len, getops.front().database, getops.front().num_records, getops.front().op);
        bgrm->clean = true;
        res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_GET, bgrm));
        getops.pop_front();
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
hxhim_return_t *hxhimFlushUnsafeGetOps(hxhim_t *hx) {
    return hxhim_return_init(hxhim::FlushGetOps(hx));
}

/**
 * FlushDeletes
 * Flushes all queued unsafe DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::Unsafe::FlushDeletes(hxhim_t *hx) {
    std::lock_guard<std::mutex> lock(hx->p->unsafe_dels.mutex);
    std::list<hxhim::unsafe_sp_t> &dels = hx->p->unsafe_dels.data;

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;

    while (dels.size()) {
        std::size_t count = std::min(dels.size(), (std::size_t) HXHIM_MAX_BULK_DEL_OPS);

        // copy the keys and lengths into arrays
        void **keys = new void *[count]();
        std::size_t *key_lens = new std::size_t[count]();
        int *databases = new int[count]();

        if (keys && key_lens) {
            for(std::size_t i = 0; i < count; i++) {
                // convert current subject+predicate into a key
                void *key = nullptr;
                std::size_t key_len = 0;

                sp_to_key(dels.front().subject, dels.front().subject_len, dels.front().predicate, dels.front().predicate_len, &key, &key_len);

                // move the constructed key into the buffer
                keys     [i] = key;
                key_lens [i] = key_len;
                databases[i] = dels.front().database;

                dels.pop_front();
            }

            res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_PUT, mdhim::BDelete(hx->p->md, nullptr, keys, key_lens, count)));

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
        delete [] databases;
    }

    return hxhim::return_results(head);
}

/**
 * FlushDeletes
 * Flushes all queued unsafe DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimUnsafeFlushDeletes(hxhim_t *hx) {
    return hxhim_return_init(hxhim::Unsafe::FlushDeletes(hx));
}

/**
 * Flush
 *     1. Do all unsafe PUTs
 *     2. Do all unsafe GETs
 *     3. Do all unsafe GET_OPs
 *     4. Do all unsafe DELs
 *
 * @param hx
 * @return An array of results (3 values)
 */
hxhim::Return *hxhim::Unsafe::Flush(hxhim_t *hx) {
    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *unsafe_puts = Unsafe::FlushPuts(hx);
    hxhim::Return *unsafe_gets = Unsafe::FlushGets(hx);
    hxhim::Return *unsafe_getops = Unsafe::FlushGetOps(hx);
    hxhim::Return *unsafe_dels = Unsafe::FlushDeletes(hx);

    head.Next(unsafe_puts)->Next(unsafe_gets)->Next(unsafe_getops)->Next(unsafe_dels);

    return hxhim::return_results(head);
}

/**
 * hxhimFlush
 * Push all queued unsafe work into MDHIM
 *
 * @param hx
 * @return An array of results (3 values)
 */
hxhim_return_t *hxhimUnsafeFlush(hxhim_t *hx) {
    return hxhim_return_init(hxhim::Unsafe::Flush(hx));
}

/**
 * UnsafePut
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the object to put
 * @param object_len   the length of the object
 * @param database     the database to put to
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::Put(hxhim_t *hx,
                       void *subject, std::size_t subject_len,
                       void *predicate, std::size_t predicate_len,
                       void *object, std::size_t object_len,
                       const int database) {
    hxhim::unsafe_spo_t spo;
    spo.subject = subject;
    spo.subject_len = subject_len;
    spo.predicate = predicate;
    spo.predicate_len = predicate_len;
    spo.object = object;
    spo.object_len = object_len;
    spo.database = database;

    std::lock_guard<std::mutex> lock(hx->p->unsafe_puts.mutex);
    hx->p->unsafe_puts.data.emplace_back(spo);
    return HXHIM_SUCCESS;
}

/**
 * hxhimUnsafePut
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the object to put
 * @param object_len   the length of the object
 * @param database     the database to put to
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafePut(hxhim_t *hx,
                   void *subject, std::size_t subject_len,
                   void *predicate, std::size_t predicate_len,
                   void *object, std::size_t object_len,
                   const int database) {
    return hxhim::Unsafe::Put(hx,
                              subject, subject_len,
                              predicate, predicate_len,
                              object, object_len,
                              database);
}

/**
 * UnsafeGet
 * Add a GET into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to get
 * @param subject_len  the length of the subject to get
 * @param prediate     the prediate to get
 * @param prediate_len the length of the prediate to get
 * @param object       the object to get
 * @param object_len   the length of the object
 * @param database     the database to get from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::Get(hxhim_t *hx,
                       void *subject, std::size_t subject_len,
                       void *predicate, std::size_t predicate_len,
                       const int database) {
    hxhim::unsafe_sp_t sp;
    sp.subject = subject;
    sp.subject_len = subject_len;
    sp.predicate = predicate;
    sp.predicate_len = predicate_len;
    sp.database = database;

    std::lock_guard<std::mutex> lock(hx->p->unsafe_gets.mutex);
    hx->p->unsafe_gets.data.emplace_back(sp);
    return HXHIM_SUCCESS;
}

/**
 * hxhimUnsafeGet
 * Add a GET into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to get
 * @param subject_len  the length of the subject to get
 * @param prediate     the prediate to get
 * @param prediate_len the length of the prediate to get
 * @param object       the object to get
 * @param object_len   the length of the object
 * @param database     the database to get from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafeGet(hxhim_t *hx,
                   void *subject, std::size_t subject_len,
                   void *predicate, std::size_t predicate_len,
                   const int database) {
    return hxhim::Unsafe::Get(hx,
                              subject, subject_len,
                              predicate, predicate_len,
                              database);
}

/**
 * UnsafeDelete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @param object       the object to delete
 * @param object_len   the length of the object
 * @param database     the database to delete from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::Delete(hxhim_t *hx,
                          void *subject, std::size_t subject_len,
                          void *predicate, std::size_t predicate_len,
                          const int database) {
    hxhim::unsafe_sp_t sp;
    sp.subject = subject;
    sp.subject_len = subject_len;
    sp.predicate = predicate;
    sp.predicate_len = predicate_len;
    sp.database = database;

    std::lock_guard<std::mutex> lock(hx->p->unsafe_dels.mutex);
    hx->p->unsafe_dels.data.emplace_back(sp);
    return HXHIM_SUCCESS;
}

/**
 * hxhimUnsafeDelete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @param object       the object to delete
 * @param object_len   the length of the object
 * @param database     the database to delete from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafeDelete(hxhim_t *hx,
                      void *subject, std::size_t subject_len,
                      void *predicate, std::size_t predicate_len,
                      const int database) {
    return hxhim::Unsafe::Delete(hx,
                                 subject, subject_len,
                                 predicate, predicate_len,
                                 database);
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
 * @param databases     the databses to put to
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::BPut(hxhim_t *hx,
                        void **subjects, std::size_t *subject_lens,
                        void **predicates, std::size_t *predicate_lens,
                        void **objects, std::size_t *object_lens,
                        const int *databases,
                        std::size_t count) {
    if (!hx || !hx->p ||
        !subjects || subject_lens ||
        !predicates || predicate_lens ||
        !objects || object_lens ||
        !databases) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        hxhim::unsafe_spo_t spo;
        spo.subject = subjects[i];
        spo.subject_len = subject_lens[i];
        spo.predicate = predicates[i];
        spo.predicate_len = predicate_lens[i];
        spo.object = objects[i];
        spo.object_len = object_lens[i];
        spo.database = databases[i];

        std::lock_guard<std::mutex> lock(hx->p->unsafe_puts.mutex);
        hx->p->unsafe_puts.data.emplace_back(spo);
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimUnsafeBPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @param databases     the databases to put to
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafeBPut(hxhim_t *hx,
                    void **subjects, std::size_t *subject_lens,
                    void **predicates, std::size_t *predicate_lens,
                    void **objects, std::size_t *object_lens,
                    int *databases,
                    std::size_t count) {
    return hxhim::Unsafe::BPut(hx,
                               subjects, subject_lens,
                               predicates, predicate_lens,
                               objects, object_lens,
                               databases,
                               count);
}

/**
 * BGet
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @param databases     the databases to get from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::BGet(hxhim_t *hx,
                        void **subjects, std::size_t *subject_lens,
                        void **predicates, std::size_t *predicate_lens,
                        const int *databases,
                        std::size_t count) {
    if (!hx || !hx->p ||
        !subjects || subject_lens ||
        !predicates || predicate_lens ||
        !databases) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        hxhim::unsafe_sp_t sp;
        sp.subject = subjects[i];
        sp.subject_len = subject_lens[i];
        sp.predicate = predicates[i];
        sp.predicate_len = predicate_lens[i];
        sp.database = databases[i];

        std::lock_guard<std::mutex> lock(hx->p->unsafe_gets.mutex);
        hx->p->unsafe_gets.data.emplace_back(sp);
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimUnsafeBGet
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @param databases     the databses to get from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafeBGet(hxhim_t *hx,
                    void **subjects, std::size_t *subject_lens,
                    void **predicates, std::size_t *predicate_lens,
                    int *databases,
                    std::size_t count) {
    return hxhim::Unsafe::BGet(hx,
                               subjects, subject_lens,
                               predicates, predicate_lens,
                               databases,
                               count);
}

/**
 * BGetOp
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @param databases     the databases to get from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::BGetOp(hxhim_t *hx,
                          void *subject, std::size_t subject_len,
                          void *predicate, std::size_t predicate_len,
                          std::size_t num_records, enum TransportGetMessageOp op,
                          const int database) {
    if (!hx || !hx->p ||
        !subject || subject_len ||
        !predicate || predicate_len) {
        return HXHIM_ERROR;
    }

    hxhim::unsafe_sp_op_t sp;
    sp.subject = subject;
    sp.subject_len = subject_len;
    sp.predicate = predicate;
    sp.predicate_len = predicate_len;
    sp.num_records = num_records;
    sp.op = op;
    sp.database = database;

    std::lock_guard<std::mutex> lock(hx->p->unsafe_getops.mutex);
    hx->p->unsafe_getops.data.emplace_back(sp);

    return HXHIM_SUCCESS;
}

/**
 * hxhimUnsafeBGetOp
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @param databases     the databses to get from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafeBGetOp(hxhim_t *hx,
                          void *subject, std::size_t subject_len,
                          void *predicate, std::size_t predicate_len,
                          std::size_t num_records, enum TransportGetMessageOp op,
                          const int database) {
    return hxhim::Unsafe::BGetOp(hx,
                                 subject, subject_len,
                                 predicate, predicate_len,
                                 num_records, op,
                                 database);
}

/**
 * BDelete
 * Add a BDELETE into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to delete
 * @param subject_lens  the lengths of the subjects to delete
 * @param prediates     the prediates to delete
 * @param prediate_lens the lengths of the prediates to delete
 * @param databases     the databases to delete from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::BDelete(hxhim_t *hx,
                           void **subjects, std::size_t *subject_lens,
                           void **predicates, std::size_t *predicate_lens,
                           const int *databases,
                           std::size_t count) {
    if (!hx || !hx->p ||
        !subjects || subject_lens ||
        !predicates || predicate_lens ||
        !databases) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        hxhim::unsafe_sp_t sp;
        sp.subject = subjects[i];
        sp.subject_len = subject_lens[i];
        sp.predicate = predicates[i];
        sp.predicate_len = predicate_lens[i];
        sp.database = databases[i];

        std::lock_guard<std::mutex> lock(hx->p->unsafe_dels.mutex);
        hx->p->unsafe_dels.data.emplace_back(sp);
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimUnsafeBDelete
 * Add a BDELETE into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to delete
 * @param subject_lens  the lengths of the subjects to delete
 * @param prediates     the prediates to delete
 * @param prediate_lens the lengths of the prediates to delete
 * @param databases     the databases to delete from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafeBDelete(hxhim_t *hx,
                       void **subjects, std::size_t *subject_lens,
                       void **predicates, std::size_t *predicate_lens,
                       int *databases,
                       std::size_t count) {
    return hxhim::Unsafe::BDelete(hx,
                                  subjects, subject_lens,
                                  predicates, predicate_lens,
                               databases,
                                  count);
}
