#include "mlog2.h"
#include "mlogfacs2.h"

#include "hxhim.h"
#include "hxhim.hpp"
#include "hxhim_private.hpp"
#include "return_private.hpp"

/**
 * FlushPuts
 * Flushes all queued unsafe PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::Unsafe::FlushPuts(hxhim_t *hx) {
    hxhim::unsafe_keyvalue_stream_t &puts = hx->p->unsafe_puts;
    std::lock_guard<std::mutex> lock(puts.mutex);

    // make sure keys and values match up
    if ((puts.keys.size()   != puts.key_lens.size())   ||
        (puts.values.size() != puts.value_lens.size()) ||
        (puts.keys.size()   != puts.values.size()))     {
        mlog(MLOG_WARN, "HXHIM Rank %d - Attempted to flush message with mismatched lengths: key (%zu), key length (%zu), value (%zu), value length (%zu). Skipping.", hx->p->md->rank, puts.keys.size(), puts.key_lens.size(), puts.values.size(), puts.value_lens.size());
        puts.clear();
        return nullptr;
    }

    Return *res = nullptr;

    // when there is only 1 set of data to operate on, use single PUT
    if (puts.keys.size() == 1) {
        res = new hxhim::Return(hxhim_work_op::HXHIM_PUT, mdhim::UnsafePut(hx->p->md, nullptr, puts.keys[0], puts.key_lens[0], puts.values[0], puts.value_lens[0], puts.databases[0]));
    }
    // use BPUT
    else {
        // copy the keys and lengths into arrays
        void **keys = new void *[puts.keys.size()]();
        std::size_t *key_lens = new std::size_t[puts.key_lens.size()]();
        void **values = new void *[puts.values.size()]();
        std::size_t *value_lens = new std::size_t[puts.value_lens.size()]();
        int *databases = new int[puts.databases.size()]();

        if (keys   && key_lens   &&
            values && value_lens &&
            databases)            {
            for(std::size_t i = 0; i < puts.keys.size(); i++) {
                keys[i] = puts.keys[i];
                key_lens[i] = puts.key_lens[i];
                values[i] = puts.values[i];
                value_lens[i] = puts.value_lens[i];
                databases[i] = puts.databases[i];
            }

            // can add some async stuff here, if keys are sorted here instead of in MDHIM

            res = new hxhim::Return(hxhim_work_op::HXHIM_PUT, mdhim::UnsafeBPut(hx->p->md, nullptr, keys, key_lens, values, value_lens, databases, puts.keys.size()));
        }

        // cleanup
        delete [] keys;
        delete [] key_lens;
        delete [] values;
        delete [] value_lens;
        delete [] databases;
    }

    puts.clear();

    return res;
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
    hxhim::unsafe_key_stream_t &gets = hx->p->unsafe_gets;
    std::lock_guard<std::mutex> lock(gets.mutex);

    // make sure keys and values match up
    if (gets.keys.size() != gets.key_lens.size()) {
        mlog(MLOG_WARN, "HXHIM Rank %d - Attempted to flush message with mismatched lengths: key (%zu) and key length (%zu). Skipping.", hx->p->md->rank, gets.keys.size(), gets.key_lens.size());
        gets.clear();
        return nullptr;
    }

    Return *res = nullptr;

    // when there is only 1 set of data to operate on, use single GET
    if (gets.keys.size() == 1) {
        res = new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::UnsafeGet(hx->p->md, nullptr, gets.keys[0], gets.key_lens[0], gets.databases[0], TransportGetMessageOp::GET_EQ));
    }
    else {
        // use BGET

        // copy the keys and lengths into arrays
        void **keys = new void *[gets.keys.size()]();
        std::size_t *key_lens = new std::size_t[gets.key_lens.size()]();
        int *databases = new int[gets.databases.size()]();

        if (keys && key_lens) {
            for(std::size_t i = 0; i < gets.keys.size(); i++) {
                keys[i] = gets.keys[i];
                key_lens[i] = gets.key_lens[i];
                databases[i] = gets.databases[i];
            }

            // can add some async stuff here, if keys are sorted here instead of in MDHIM

            res = new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::UnsafeBGet(hx->p->md, nullptr, keys, key_lens, databases, gets.keys.size(), TransportGetMessageOp::GET_EQ));
        }

        // cleanup
        delete [] keys;
        delete [] key_lens;
        delete [] databases;
    }

    gets.clear();

    return res;
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
 * FlushDeletes
 * Flushes all queued unsafe DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::Unsafe::FlushDeletes(hxhim_t *hx) {
    hxhim::unsafe_key_stream_t &dels = hx->p->unsafe_dels;
    std::lock_guard<std::mutex> lock(dels.mutex);

    // make sure keys and values match up
    if (dels.keys.size() != dels.key_lens.size()) {
        mlog(MLOG_WARN, "HXHIM Rank %d - Attempted to flush message with mismatched lengths: key (%zu) and key length (%zu). Skipping.", hx->p->md->rank, dels.keys.size(), dels.key_lens.size());
        dels.clear();
        return nullptr;
    }

    Return *res = nullptr;

    // when there is only 1 set of data to operate on, use single DEL
    if (dels.keys.size() == 1) {
        res = new hxhim::Return(hxhim_work_op::HXHIM_DEL, mdhim::UnsafeDelete(hx->p->md, nullptr, dels.keys[0], dels.key_lens[0], dels.databases[0]));
    }
    else {
        // use BDEL

        // copy the keys and lengths into arrays
        void **keys = new void *[dels.keys.size()]();
        std::size_t *key_lens = new std::size_t[dels.key_lens.size()]();
        int *databases = new int[dels.databases.size()]();

        if (keys && key_lens) {
            for(std::size_t i = 0; i < dels.keys.size(); i++) {
                keys[i] = dels.keys[i];
                key_lens[i] = dels.key_lens[i];
                databases[i] = dels.databases[i];
            }

            // can add some async stuff here, if keys are sorted here instead of in MDHIM

            res = new hxhim::Return(hxhim_work_op::HXHIM_DEL, mdhim::UnsafeBDelete(hx->p->md, nullptr, keys, key_lens, databases, dels.keys.size()));
        }

        // cleanup
        delete [] keys;
        delete [] key_lens;
        delete [] databases;
    }

    dels.clear();

    return res;
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
 *     3. Do all unsafe DELs
 *
 * @param hx
 * @return An array of results (3 values)
 */
hxhim::Return *hxhim::Unsafe::Flush(hxhim_t *hx) {
    hxhim::Return *unsafe_puts = Unsafe::FlushPuts(hx);
    hxhim::Return *unsafe_gets = Unsafe::FlushGets(hx);
    hxhim::Return *unsafe_dels = Unsafe::FlushDeletes(hx);

    unsafe_puts->Next(unsafe_gets);
    unsafe_gets->Next(unsafe_dels);
    return unsafe_puts;
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
 * @param hx        the HXHIM session
 * @param key       the key to put
 * @param key_len   the length of the key to put
 * @param value     the value associated with the key
 * @param value_len the length of the value
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::Put(hxhim_t *hx, void *key, std::size_t key_len, void *value, std::size_t value_len, const int database) {
    if (!hx || !hx->p || !key || !value) {
        return HXHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->unsafe_puts.mutex);
    hx->p->unsafe_puts.keys.push_back(key);
    hx->p->unsafe_puts.key_lens.push_back(key_len);
    hx->p->unsafe_puts.values.push_back(value);
    hx->p->unsafe_puts.value_lens.push_back(value_len);
    hx->p->unsafe_puts.databases.push_back(database);

    return HXHIM_SUCCESS;
}

/**
 * hxhimUnsafePut
 * Add a PUT into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to put
 * @param key_len   the length of the key to put
 * @param value     the value associated with the key
 * @param value_len the length of the value
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafePut(hxhim_t *hx, void *key, std::size_t key_len, void *value, std::size_t value_len, const int database) {
    return hxhim::Unsafe::Put(hx, key, key_len, value, value_len, database);
}

/**
 * UnsafeGet
 * Add a GET into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to get
 * @param key_len   the length of the key to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::Get(hxhim_t *hx, void *key, std::size_t key_len, const int database) {
    if (!hx || !hx->p || !key) {
        return HXHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->unsafe_gets.mutex);
    hx->p->unsafe_gets.keys.push_back(key);
    hx->p->unsafe_gets.key_lens.push_back(key_len);
    hx->p->unsafe_gets.databases.push_back(database);

    return HXHIM_SUCCESS;
}

/**
 * hxhimUnsafeGet
 * Add a GET into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to get
 * @param key_len   the length of the key to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafeGet(hxhim_t *hx, void *key, std::size_t key_len, const int database) {
    return hxhim::Unsafe::Get(hx, key, key_len, database);
}

/**
 * UnsafeDelete
 * Add a DEL into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to del
 * @param key_len   the length of the key to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::Delete(hxhim_t *hx, void *key, std::size_t key_len, const int database) {
    if (!hx || !hx->p || !key) {
        return HXHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->unsafe_dels.mutex);
    hx->p->unsafe_dels.keys.push_back(key);
    hx->p->unsafe_dels.key_lens.push_back(key_len);
    hx->p->unsafe_dels.databases.push_back(database);

    return HXHIM_SUCCESS;
}

/**
 * hxhimUnsafeDelete
 * Add a DEL into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to del
 * @param key_len   the length of the key to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafeDelete(hxhim_t *hx, void *key, std::size_t key_len, const int database) {
    return hxhim::Unsafe::Delete(hx, key, key_len, database);
}

/**
 * UnsafeBPut
 * Add a BPUT into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bput
 * @param key_lens   the length of the keys to bput
 * @param values     the values associated with the keys
 * @param value_lens the length of the values
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::BPut(hxhim_t *hx, void **keys, std::size_t *key_lens, void **values, std::size_t *value_lens, const int *databases, std::size_t num_keys) {
    if (!hx || !hx->p || !keys || !key_lens || !values || !value_lens) {
        return HXHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->unsafe_puts.mutex);
    for(std::size_t i = 0; i < num_keys; i++) {
        hx->p->unsafe_puts.keys.push_back(keys[i]);
        hx->p->unsafe_puts.key_lens.push_back(key_lens[i]);
        hx->p->unsafe_puts.values.push_back(values[i]);
        hx->p->unsafe_puts.value_lens.push_back(value_lens[i]);
        hx->p->unsafe_puts.databases.push_back(databases[i]);
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimUnsafeBPut
 * Add a BPUT into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bput
 * @param key_lens   the length of the keys to bput
 * @param values     the values associated with the keys
 * @param value_lens the length of the values
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafeBPut(hxhim_t *hx, void **keys, std::size_t *key_lens, void **values, std::size_t *value_lens, const int *databases, std::size_t num_keys) {
    return hxhim::Unsafe::BPut(hx, keys, key_lens, values, value_lens, databases, num_keys);
}

/**
 * UnsafeBGet
 * Add a BGET into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bget
 * @param key_lens   the length of the keys to bget
 * @return A wrapped return value containing responses
 */
int hxhim::Unsafe::BGet(hxhim_t *hx, void **keys, std::size_t *key_lens, const int *databases, std::size_t num_keys) {
    if (!hx || !hx->p || !keys || !key_lens) {
        return MDHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->unsafe_gets.mutex);
    for(std::size_t i = 0; i < num_keys; i++) {
        hx->p->unsafe_gets.keys.push_back(keys[i]);
        hx->p->unsafe_gets.key_lens.push_back(key_lens[i]);
        hx->p->unsafe_gets.databases.push_back(databases[i]);
    }

    return MDHIM_SUCCESS;
}

/**
 * hxhimUnsafeBGet
 * Add a BGET into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bget
 * @param key_lens   the length of the keys to bget
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafeBGet(hxhim_t *hx, void **keys, std::size_t *key_lens, const int *databases, std::size_t num_keys) {
    return hxhim::Unsafe::BGet(hx, keys, key_lens, databases, num_keys);
}

/**
 * UnsafeBDelete
 * Add a BDEL into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bdel
 * @param key_lens   the length of the keys to bdelete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Unsafe::BDelete(hxhim_t *hx, void **keys, std::size_t *key_lens, const int *databases, std::size_t num_keys) {
    if (!hx || !hx->p || !keys || !key_lens) {
        return HXHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->unsafe_dels.mutex);
    for(std::size_t i = 0; i < num_keys; i++) {
        hx->p->unsafe_dels.keys.push_back(keys[i]);
        hx->p->unsafe_dels.key_lens.push_back(key_lens[i]);
        hx->p->unsafe_dels.databases.push_back(databases[i]);
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimUnsafeBDelete
 * Add a BDEL into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bdel
 * @param key_lens   the length of the keys to bdelete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimUnsafeBDelete(hxhim_t *hx, void **keys, std::size_t *key_lens, const int *databases, std::size_t num_keys) {
    return hxhim::Unsafe::BDelete(hx, keys, key_lens, databases, num_keys);
}
