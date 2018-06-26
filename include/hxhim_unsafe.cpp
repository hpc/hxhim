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
 * FlushPuts
 * Flushes all queued unsafe PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::Unsafe::FlushPuts(hxhim_t *hx) {
    hxhim::Unsent<hxhim::UnsafePutData> &puts = hx->p->unsafe_puts;
    std::lock_guard<std::mutex> lock(puts.mutex);

    hxhim::UnsafePutData *curr = puts.head;
    if (!curr) {
        return HXHIM_SUCCESS;
    }

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;

    void **keys = nullptr;
    std::size_t *key_lens = nullptr;
    void **values = nullptr;
    std::size_t *value_lens = nullptr;
    std::size_t offset = 0;

    static const std::size_t multiplier = 4;

    // write complete batches
    while (curr->next) {
        // Generate up to 1 MAX_BULK_OPS worth of data
        static const std::size_t total = multiplier * HXHIM_MAX_BULK_PUT_OPS;

        keys = new void *[total]();
        key_lens = new std::size_t[total]();
        values = new void *[total]();
        value_lens = new std::size_t[total]();

        offset = 0;
        for(std::size_t i = 0; i < HXHIM_MAX_BULK_PUT_OPS; i++) {
            sp_to_key(curr->subjects[i], curr->subject_lens[i], curr->predicates[i], curr->predicate_lens[i], &keys[offset], &key_lens[offset]);
            values[offset] = curr->objects[i];
            value_lens[offset] = curr->object_lens[i];
            offset++;

            sp_to_key(curr->subjects[i], curr->subject_lens[i], curr->objects[i], curr->object_lens[i], &keys[offset], &key_lens[offset]);
            values[offset] = curr->predicates[i];
            value_lens[offset] = curr->predicate_lens[i];
            offset++;

            sp_to_key(curr->predicates[i], curr->predicate_lens[i], curr->objects[i], curr->object_lens[i], &keys[offset], &key_lens[offset]);
            values[offset] = curr->subjects[i];
            value_lens[offset] = curr->subject_lens[i];
            offset++;

            sp_to_key(curr->predicates[i], curr->predicate_lens[i], curr->subjects[i], curr->subject_lens[i], &keys[offset], &key_lens[offset]);
            values[offset] = curr->objects[i];
            value_lens[offset] = curr->object_lens[i];
            offset++;
        }

        // PUT the batch
        res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_PUT, mdhim::Unsafe::BPut(hx->p->md, nullptr, keys, key_lens, values, value_lens, curr->databases, total)));

        // cleanup
        for(std::size_t i = 0; i < total; i++) {
            ::operator delete(keys[i]);
        }
        delete [] keys;
        delete [] key_lens;
        delete [] values;
        delete [] value_lens;

        // go to the next batch
        hxhim::UnsafePutData *next = curr->next;
        delete curr;
        curr = next;
    }

    // write final (likely incomplete) batch
    const std::size_t total = multiplier * puts.last_count;

    keys = new void *[total]();
    key_lens = new std::size_t[total]();
    values = new void *[total]();
    value_lens = new std::size_t[total]();

    offset = 0;
    for(std::size_t i = 0; i < puts.last_count; i++) {
        sp_to_key(curr->subjects[i], curr->subject_lens[i], curr->predicates[i], curr->predicate_lens[i], &keys[offset], &key_lens[offset]);
        values[offset] = curr->objects[i];
        value_lens[offset] = curr->object_lens[i];
        offset++;

        sp_to_key(curr->subjects[i], curr->subject_lens[i], curr->objects[i], curr->object_lens[i], &keys[offset], &key_lens[offset]);
        values[offset] = curr->predicates[i];
        value_lens[offset] = curr->predicate_lens[i];
        offset++;

        sp_to_key(curr->predicates[i], curr->predicate_lens[i], curr->objects[i], curr->object_lens[i], &keys[offset], &key_lens[offset]);
        values[offset] = curr->subjects[i];
        value_lens[offset] = curr->subject_lens[i];
        offset++;

        sp_to_key(curr->predicates[i], curr->predicate_lens[i], curr->subjects[i], curr->subject_lens[i], &keys[offset], &key_lens[offset]);
        values[offset] = curr->objects[i];
        value_lens[offset] = curr->object_lens[i];
        offset++;
    }

    // PUT the batch
    res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_PUT, mdhim::Unsafe::BPut(hx->p->md, nullptr, keys, key_lens, values, value_lens, curr->databases, total)));

    // cleanup
    for(std::size_t i = 0; i < total; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;
    delete [] key_lens;
    delete [] values;
    delete [] value_lens;

    // delete the last batch
    delete curr;

    puts.head = puts.tail = nullptr;

    return hxhim::return_results(head);
}

/**
 * hxhimFlushUnsafePuts
 * Flushes all queued unsafe PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushUnsafePuts(hxhim_t *hx) {
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
    hxhim::Unsent<hxhim::UnsafeGetData> &gets = hx->p->unsafe_gets;
    std::lock_guard<std::mutex> lock(gets.mutex);

    hxhim::UnsafeGetData *curr = gets.head;
    if (!curr) {
        return HXHIM_SUCCESS;
    }

    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *res = &head;

    void **keys = nullptr;
    std::size_t *key_lens = nullptr;
    int *databases = nullptr;

    // write complete batches
    while (curr->next) {
        keys = new void *[HXHIM_MAX_BULK_GET_OPS]();
        key_lens = new std::size_t[HXHIM_MAX_BULK_GET_OPS]();

        for(std::size_t i = 0; i < HXHIM_MAX_BULK_GET_OPS; i++) {
            sp_to_key(curr->subjects[i], curr->subject_lens[i], curr->predicates[i], curr->predicate_lens[i], &keys[i], &key_lens[i]);
        }

        // GET the batch
        res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::Unsafe::BGet(hx->p->md, nullptr, keys, key_lens, curr->databases, HXHIM_MAX_BULK_GET_OPS, TransportGetMessageOp::GET_EQ)));

        // cleanup
        for(std::size_t i = 0; i < HXHIM_MAX_BULK_GET_OPS; i++) {
            ::operator delete(keys[i]);
        }
        delete [] keys;
        delete [] key_lens;

        // go to the next batch
        hxhim::UnsafeGetData *next = curr->next;
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
    res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::Unsafe::BGet(hx->p->md, nullptr, keys, key_lens, curr->databases, gets.last_count, TransportGetMessageOp::GET_EQ)));

    // cleanup
    for(std::size_t i = 0; i < gets.last_count; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;
    delete [] key_lens;
    delete [] databases;

    // delete the last batch
    delete curr;

    gets.head = gets.tail = nullptr;

    return hxhim::return_results(head);
}

/**
 * hxhimFlushUnsafeGets
 * Flushes all queued unsafe GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushUnsafeGets(hxhim_t *hx) {
    return hxhim_return_init(hxhim::Unsafe::FlushGets(hx));
}

/**
 * FlushGetOps
 * Flushes all queued unsafe GETs with specific operations
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::Unsafe::FlushGetOps(hxhim_t *hx) {
    hxhim::Unsent<hxhim::UnsafeGetOpData> &getops = hx->p->unsafe_getops;
    std::lock_guard<std::mutex> lock(getops.mutex);

    hxhim::UnsafeGetOpData *curr = getops.head;
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
            res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::Unsafe::BGetOp(hx->p->md, nullptr, key, key_len, curr->databases[i], HXHIM_MAX_BULK_GET_OPS, curr->ops[i])));
            ::operator delete(key);
        }

        // go to the next batch
        hxhim::UnsafeGetOpData *next = curr->next;
        delete curr;
        curr = next;
    }

    // write final (likely incomplete) batch
    for(std::size_t i = 0; i < getops.last_count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(curr->subjects[i], curr->subject_lens[i], curr->predicates[i], curr->predicate_lens[i], &key, &key_len);

        // GETOP the key
        res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::Unsafe::BGetOp(hx->p->md, nullptr, key, key_len, curr->databases[i], getops.last_count, curr->ops[i])));
        ::operator delete(key);
    }

    // delete the last batch
    delete curr;

    getops.head = getops.tail = nullptr;

    return hxhim::return_results(head);
}

/**
 * hxhimFlushUnsafeGetOps
 * Flushes all queued unsafe GETs with specific operations
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushUnsafeGetOps(hxhim_t *hx) {
    return hxhim_return_init(hxhim::Unsafe::FlushGetOps(hx));
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
    hxhim::Unsent<hxhim::UnsafeDeleteData> &dels = hx->p->unsafe_deletes;
    std::lock_guard<std::mutex> lock(dels.mutex);

    hxhim::UnsafeDeleteData *curr = dels.head;
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
        res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_DEL, mdhim::Unsafe::BDelete(hx->p->md, nullptr, keys, key_lens, curr->databases, HXHIM_MAX_BULK_DEL_OPS)));

        // cleanup
        for(std::size_t i = 0; i < HXHIM_MAX_BULK_DEL_OPS; i++) {
            ::operator delete(keys[i]);
        }
        delete [] keys;
        delete [] key_lens;

        // go to the next batch
        hxhim::UnsafeDeleteData *next = curr->next;
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
    res = res->Next(new hxhim::Return(hxhim_work_op::HXHIM_DEL, mdhim::Unsafe::BDelete(hx->p->md, nullptr, keys, key_lens, curr->databases, dels.last_count)));

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
 * hxhimFlushUnsafeDeletes
 * Flushes all queued unsafe DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_return_t *hxhimFlushUnsafeDeletes(hxhim_t *hx) {
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
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::Unsafe::Flush(hxhim_t *hx) {
    hxhim::Return head(hxhim_work_op::HXHIM_NOP, nullptr);
    hxhim::Return *curr = &head;

    combine_results(curr, Unsafe::FlushPuts(hx));
    combine_results(curr, Unsafe::FlushGets(hx));
    combine_results(curr, Unsafe::FlushGetOps(hx));
    combine_results(curr, Unsafe::FlushDeletes(hx));

    return hxhim::return_results(head);
}

/**
 * hxhimUnsafeFlush
 * Push all queued work into MDHIM
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
    if (!hx        || !hx->p         ||
        !subject   || !subject_len   ||
        !predicate || !predicate_len ||
        !object    || !object_len)    {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::UnsafePutData> &puts = hx->p->unsafe_puts;
    std::lock_guard<std::mutex> lock(puts.mutex);

    // no previous batch
    if (!puts.tail) {
        puts.head       = new hxhim::UnsafePutData();
        puts.tail       = puts.head;
        puts.last_count = 0;
    }

    // filled the current batch
    if (puts.last_count == HXHIM_MAX_BULK_PUT_OPS) {
        puts.tail->next = new hxhim::UnsafePutData();
        puts.tail       = puts.tail->next;
        puts.last_count = 0;
    }

    std::size_t &i = puts.last_count;
    puts.tail->subjects[i] = subject;
    puts.tail->subject_lens[i] = subject_len;
    puts.tail->predicates[i] = predicate;
    puts.tail->predicate_lens[i] = predicate_len;
    puts.tail->objects[i] = object;
    puts.tail->object_lens[i] = object_len;
    puts.tail->databases[i] = database;
    i++;

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
 * @param database     the database to send to
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::Get(hxhim_t *hx,
                       void *subject, std::size_t subject_len,
                       void *predicate, std::size_t predicate_len,
                       const int database) {
    if (!hx        || !hx->p         ||
        !subject   || !subject_len   ||
        !predicate || !predicate_len) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::UnsafeGetData> &gets = hx->p->unsafe_gets;
    std::lock_guard<std::mutex> lock(gets.mutex);

    // no previous batch
    if (!gets.tail) {
        gets.head       = new hxhim::UnsafeGetData();
        gets.tail       = gets.head;
        gets.last_count = 0;
    }

    // filled the current batch
    if (gets.last_count == HXHIM_MAX_BULK_GET_OPS) {
        gets.tail->next = new hxhim::UnsafeGetData();
        gets.tail       = gets.tail->next;
        gets.last_count = 0;
    }

    std::size_t &i = gets.last_count;
    gets.tail->subjects[i] = subject;
    gets.tail->subject_lens[i] = subject_len;
    gets.tail->predicates[i] = predicate;
    gets.tail->predicate_lens[i] = predicate_len;
    gets.tail->databases[i] = database;
    i++;

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
 * @param database     the database to delete from
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
 * Delete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @param database     the database to delete from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::Delete(hxhim_t *hx,
                          void *subject, std::size_t subject_len,
                          void *predicate, std::size_t predicate_len,
                          const int database) {
    if (!hx        || !hx->p         ||
        !subject   || !subject_len   ||
        !predicate || !predicate_len) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::UnsafeDeleteData> &dels = hx->p->unsafe_deletes;
    std::lock_guard<std::mutex> lock(dels.mutex);

    // no previous batch
    if (!dels.tail) {
        dels.head       = new hxhim::UnsafeDeleteData();
        dels.tail       = dels.head;
        dels.last_count = 0;
    }

    // filled the current batch
    if (dels.last_count == HXHIM_MAX_BULK_DEL_OPS) {
        dels.tail->next = new hxhim::UnsafeDeleteData();
        dels.tail       = dels.tail->next;
        dels.last_count = 0;
    }

    std::size_t &i = dels.last_count;
    dels.tail->subjects[i] = subject;
    dels.tail->subject_lens[i] = subject_len;
    dels.tail->predicates[i] = predicate;
    dels.tail->predicate_lens[i] = predicate_len;
    dels.tail->databases[i] = database;
    i++;

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
 * @param object       the object associated with the key
 * @param object_len   the length of the object
 * @param database     the database to send to
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
 * @param databases     the databases to put to
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::BPut(hxhim_t *hx,
                        void **subjects, std::size_t *subject_lens,
                        void **predicates, std::size_t *predicate_lens,
                        void **objects, std::size_t *object_lens,
                        const int *databases, std::size_t count) {
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !objects    || !object_lens    ||
        !databases) {
        return HXHIM_ERROR;
    }

    if (count) {
        hxhim::Unsent<hxhim::UnsafePutData> &puts = hx->p->unsafe_puts;
        std::lock_guard<std::mutex> lock(puts.mutex);

        // no previous batch
        if (!puts.tail) {
            puts.head       = new hxhim::UnsafePutData();
            puts.tail       = puts.head;
            puts.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (puts.last_count == HXHIM_MAX_BULK_PUT_OPS) {
                puts.tail->next = new hxhim::UnsafePutData();
                puts.tail       = puts.tail->next;
                puts.last_count = 0;
            }

            std::size_t &i = puts.last_count;
            puts.tail->subjects[i] = subjects[c];
            puts.tail->subject_lens[i] = subject_lens[c];
            puts.tail->predicates[i] = predicates[c];
            puts.tail->predicate_lens[i] = predicate_lens[c];
            puts.tail->objects[i] = objects[c];
            puts.tail->object_lens[i] = object_lens[c];
            puts.tail->databases[i] = databases[c];
            i++;
        }
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
                    const int *databases, std::size_t count) {
    return hxhim::Unsafe::BPut(hx,
                               subjects, subject_lens,
                               predicates, predicate_lens,
                               objects, object_lens,
                               databases, count);
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
                        const int *databases, std::size_t count) {
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !databases) {
        return HXHIM_ERROR;
    }

    if (count) {
        hxhim::Unsent<hxhim::UnsafeGetData> &gets = hx->p->unsafe_gets;
        std::lock_guard<std::mutex> lock(gets.mutex);

        // no previous batch
        if (!gets.tail) {
            gets.head     = new hxhim::UnsafeGetData();
            gets.tail     = gets.head;
            gets.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (gets.last_count == HXHIM_MAX_BULK_GET_OPS) {
                gets.tail->next = new hxhim::UnsafeGetData();
                gets.tail       = gets.tail->next;
                gets.last_count = 0;
            }

            std::size_t &i = gets.last_count;
            gets.tail->subjects[i] = subjects[c];
            gets.tail->subject_lens[i] = subject_lens[c];
            gets.tail->predicates[i] = predicates[c];
            gets.tail->predicate_lens[i] = predicate_lens[c];
            gets.tail->databases[i] = databases[c];
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
 * @param databases     the databases to get from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafeBGet(hxhim_t *hx,
                    void **subjects, std::size_t *subject_lens,
                    void **predicates, std::size_t *predicate_lens,
                    const int *databases, std::size_t count) {
    return hxhim::Unsafe::BGet(hx,
                               subjects, subject_lens,
                               predicates, predicate_lens,
                               databases, count);
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
 * @param database      the database to get from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::BGetOp(hxhim_t *hx,
                          void *subject, std::size_t subject_len,
                          void *predicate, std::size_t predicate_len,
                          std::size_t num_records, enum TransportGetMessageOp op,
                          const int database) {
    if (!hx        || !hx->p         ||
        !subject   || !subject_len   ||
        !predicate || !predicate_len) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::UnsafeGetOpData> &getops = hx->p->unsafe_getops;
    std::lock_guard<std::mutex> lock(getops.mutex);

    // no previous batch
    if (!getops.tail) {
        getops.head       = new hxhim::UnsafeGetOpData();
        getops.tail       = getops.head;
        getops.last_count = 0;
    }

    // filled the current batch
    if (getops.last_count == HXHIM_MAX_BULK_GET_OPS) {
        getops.tail->next = new hxhim::UnsafeGetOpData();
        getops.tail       = getops.tail->next;
        getops.last_count = 0;
    }

    std::size_t &i = getops.last_count;
    getops.tail->subjects[i] = subject;
    getops.tail->subject_lens[i] = subject_len;
    getops.tail->predicates[i] = predicate;
    getops.tail->predicate_lens[i] = predicate_len;
    getops.tail->ops[i] = op;
    getops.tail->databases[i] = database;
    i++;

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
 * @param database      the database to get from
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
                           const int *databases, std::size_t count) {
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !databases) {
        return HXHIM_ERROR;
    }

    if (count) {
        hxhim::Unsent<hxhim::UnsafeDeleteData> &dels = hx->p->unsafe_deletes;
        std::lock_guard<std::mutex> lock(dels.mutex);

        // no previous batch
        if (!dels.tail) {
            dels.head       = new hxhim::UnsafeDeleteData();
            dels.tail       = dels.head;
            dels.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (dels.last_count == HXHIM_MAX_BULK_DEL_OPS) {
                dels.tail->next = new hxhim::UnsafeDeleteData();
                dels.tail       = dels.tail->next;
                dels.last_count = 0;
            }

            std::size_t &i = dels.last_count;
            dels.tail->subjects[i] = subjects[c];
            dels.tail->subject_lens[i] = subject_lens[c];
            dels.tail->predicates[i] = predicates[c];
            dels.tail->predicate_lens[i] = predicate_lens[c];
            dels.tail->databases[i] = databases[c];
            i++;
        }
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
                       const int *databases, std::size_t count) {
    return hxhim::Unsafe::BDelete(hx,
                                  subjects, subject_lens,
                                  predicates, predicate_lens,
                                  databases, count);
}
