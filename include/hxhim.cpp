#include "mlog2.h"
#include "mlogfacs2.h"
#include <iostream>

#include "hxhim.h"
#include "hxhim.hpp"
#include "hxhim_private.hpp"
#include "return_private.hpp"

/**
 * config_reader
 * Opens a configuration file
 *
 * @param opts           the MDHIM options to fill in
 * @param bootstrap_comm the MPI communicator used to boostrap MDHIM
 * @param filename       the name of the file to open (for now, it is only the mdhim configuration)
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
static int config_reader(mdhim_options_t *opts, const MPI_Comm bootstrap_comm, const std::string &filename) {
    ConfigSequence config_sequence;

    ConfigFile file(filename);
    config_sequence.add(&file);

    if ((mdhim_options_init(opts, bootstrap_comm, false, false) != MDHIM_SUCCESS) || // initialize opts->p, opts->p->transport, and opts->p->db
        (process_config_and_fill_options(config_sequence, opts) != MDHIM_SUCCESS)) { // read the configuration and overwrite default values
        mdhim_options_destroy(opts);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
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
int hxhim::Open(hxhim_t *hx, const MPI_Comm bootstrap_comm, const std::string &filename) {
    if (!hx) {
        return HXHIM_ERROR;
    }

    if (!(hx->p = new hxhim_private_t())) {
        return HXHIM_ERROR;
    }

    // initialize options through config
    const std::string mdhim_config = filename; // the mdhim_config value should be part of the hxhim configuration
    if (!(hx->p->mdhim_opts = new mdhim_options_t())                                            ||
        (config_reader(hx->p->mdhim_opts, bootstrap_comm, mdhim_config) != MDHIM_SUCCESS)) {
        Close(hx);
        return HXHIM_ERROR;
    }

    // initialize mdhim context
    if (!(hx->p->md = new mdhim_t())                               ||
        (mdhim::Init(hx->p->md, hx->p->mdhim_opts) != MDHIM_SUCCESS)) {
        Close(hx);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
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
int hxhimOpen(hxhim_t *hx, const MPI_Comm bootstrap_comm, const char *filename) {
    return hxhim::Open(hx, bootstrap_comm, std::string(filename, strlen(filename)));
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
    hx->p->puts.clear();

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
 * FlushPuts
 * Flushes all queued PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::FlushPuts(hxhim_t *hx) {
    hxhim::keyvalue_stream_t &puts = hx->p->puts;
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
        res = new hxhim::Return(hxhim_work_op::HXHIM_PUT, mdhim::Put(hx->p->md, nullptr, puts.keys[0], puts.key_lens[0], puts.values[0], puts.value_lens[0]));
    }
    // use BPUT
    else {
        // copy the keys and lengths into arrays
        void **keys = new void *[puts.keys.size()]();
        std::size_t *key_lens = new std::size_t[puts.key_lens.size()]();
        void **values = new void *[puts.values.size()]();
        std::size_t *value_lens = new std::size_t[puts.value_lens.size()]();

        if (keys   && key_lens   &&
            values && value_lens) {
            for(std::size_t i = 0; i < puts.keys.size(); i++) {
                keys[i] = puts.keys[i];
                key_lens[i] = puts.key_lens[i];
                values[i] = puts.values[i];
                value_lens[i] = puts.value_lens[i];
            }

            // can add some async stuff here, if keys are sorted here instead of in MDHIM

            res = new hxhim::Return(hxhim_work_op::HXHIM_PUT, mdhim::BPut(hx->p->md, nullptr, keys, key_lens, values, value_lens, puts.keys.size()));
        }

        // cleanup
        delete [] keys;
        delete [] key_lens;
        delete [] values;
        delete [] value_lens;
    }

    puts.clear();

    return res;
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
    hxhim::key_stream_t &gets = hx->p->gets;
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
        res = new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::Get(hx->p->md, nullptr, gets.keys[0], gets.key_lens[0], TransportGetMessageOp::GET_EQ));
    }
    else {
        // use BGET

        // copy the keys and lengths into arrays
        void **keys = new void *[gets.keys.size()]();
        std::size_t *key_lens = new std::size_t[gets.key_lens.size()]();

        if (keys && key_lens) {
            for(std::size_t i = 0; i < gets.keys.size(); i++) {
                keys[i] = gets.keys[i];
                key_lens[i] = gets.key_lens[i];
            }

            // can add some async stuff here, if keys are sorted here instead of in MDHIM

            res = new hxhim::Return(hxhim_work_op::HXHIM_GET, mdhim::BGet(hx->p->md, nullptr, keys, key_lens, gets.keys.size(), TransportGetMessageOp::GET_EQ));
        }

        // cleanup
        delete [] keys;
        delete [] key_lens;
    }

    gets.clear();

    return res;
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
 * FlushDeletes
 * Flushes all queued DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Return *hxhim::FlushDeletes(hxhim_t *hx) {
    hxhim::key_stream_t &dels = hx->p->dels;
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
        res = new hxhim::Return(hxhim_work_op::HXHIM_DEL, mdhim::Delete(hx->p->md, nullptr, dels.keys[0], dels.key_lens[0]));
    }
    else {
        // use BDEL

        // copy the keys and lengths into arrays
        void **keys = new void *[dels.keys.size()]();
        std::size_t *key_lens = new std::size_t[dels.key_lens.size()]();

        if (keys && key_lens) {
            for(std::size_t i = 0; i < dels.keys.size(); i++) {
                keys[i] = dels.keys[i];
                key_lens[i] = dels.key_lens[i];
            }

            // can add some async stuff here, if keys are sorted here instead of in MDHIM

            res = new hxhim::Return(hxhim_work_op::HXHIM_DEL, mdhim::BDelete(hx->p->md, nullptr, keys, key_lens, dels.keys.size()));
        }

        // cleanup
        delete [] keys;
        delete [] key_lens;
    }

    dels.clear();

    return res;
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
 *     3. Do all DELs
 *
 * @param hx
 * @return An array of results (3 values)
 */
hxhim::Return **hxhim::Flush(hxhim_t *hx) {
    // All return Return *
    // Put these into an array
    hxhim::Return **res = new hxhim::Return *[HXHIM_RESULTS_SIZE]();

    res[0] = FlushPuts(hx);
    res[1] = FlushGets(hx);
    res[2] = FlushDeletes(hx);

    return res;
}

/**
 * hxhimFlush
 * Push all queued work into MDHIM
 *
 * @param hx
 * @return An array of results (3 values)
 */
hxhim_return_t **hxhimFlush(hxhim_t *hx) {
    hxhim::Return **res = hxhim::Flush(hx);
    hxhim_return_t **ret = new hxhim_return_t *[HXHIM_RESULTS_SIZE]();
    for(int i = 0; i < HXHIM_RESULTS_SIZE; i++) {
        ret[i] = hxhim_return_init(res[i]);
    }
    delete [] res;
    return ret;
}

/**
 * DestroyFlush
 * Deallocate the result array of flush
 *
 * @param res the array of 3 results from Flush
 */
void hxhim::DestroyFlush(hxhim::Return **res) {
    if (res) {
        for(int i = 0; i < HXHIM_RESULTS_SIZE; i++) {
            delete res[i];
        }

        delete [] res;
    }
}

/**
 * hxhimDestroyFlush
 * Deallocate the result array of flush
 *
 * @param res the array of 3 results from hxhimFlush
 */
void hxhimDestroyFlush(hxhim_return_t **ret) {
    if (ret) {
        for(int i = 0; i < HXHIM_RESULTS_SIZE; i++) {
            hxhim_return_destroy(ret[i]);
        }

        delete [] ret;
    }
}

/**
 * Put
 * Add a PUT into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to put
 * @param key_len   the length of the key to put
 * @param value     the value associated with the key
 * @param value_len the length of the value
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Put(hxhim_t *hx, void *key, std::size_t key_len, void *value, std::size_t value_len) {
    if (!hx || !hx->p || !key || !value) {
        return HXHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->puts.mutex);
    hx->p->puts.keys.push_back(key);
    hx->p->puts.key_lens.push_back(key_len);
    hx->p->puts.values.push_back(value);
    hx->p->puts.value_lens.push_back(value_len);

    return HXHIM_SUCCESS;
}

/**
 * hxhimPut
 * Add a PUT into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to put
 * @param key_len   the length of the key to put
 * @param value     the value associated with the key
 * @param value_len the length of the value
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimPut(hxhim_t *hx, void *key, std::size_t key_len, void *value, std::size_t value_len) {
    return hxhim::Put(hx, key, key_len, value, value_len);
}

/**
 * Get
 * Add a GET into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to get
 * @param key_len   the length of the key to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Get(hxhim_t *hx, void *key, std::size_t key_len) {
    if (!hx || !hx->p || !key) {
        return HXHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->gets.mutex);
    hx->p->gets.keys.push_back(key);
    hx->p->gets.key_lens.push_back(key_len);

    return HXHIM_SUCCESS;
}

/**
 * hxhimGet
 * Add a GET into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to get
 * @param key_len   the length of the key to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGet(hxhim_t *hx, void *key, std::size_t key_len) {
    return hxhim::Get(hx, key, key_len);
}

/**
 * Delete
 * Add a DEL into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to del
 * @param key_len   the length of the key to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Delete(hxhim_t *hx, void *key, std::size_t key_len) {
    if (!hx || !hx->p || !key) {
        return HXHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->dels.mutex);
    hx->p->dels.keys.push_back(key);
    hx->p->dels.key_lens.push_back(key_len);

    return HXHIM_SUCCESS;
}

/**
 * hxhimDelete
 * Add a DEL into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to del
 * @param key_len   the length of the key to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimDelete(hxhim_t *hx, void *key, std::size_t key_len) {
    return hxhim::Delete(hx, key, key_len);
}

/**
 * BPut
 * Add a BPUT into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bput
 * @param key_lens   the length of the keys to bput
 * @param values     the values associated with the keys
 * @param value_lens the length of the values
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BPut(hxhim_t *hx, void **keys, std::size_t *key_lens, void **values, std::size_t *value_lens, std::size_t num_keys) {
    if (!hx || !hx->p || !keys || !key_lens || !values || !value_lens) {
        return HXHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->puts.mutex);
    for(std::size_t i = 0; i < num_keys; i++) {
        hx->p->puts.keys.push_back(keys[i]);
        hx->p->puts.key_lens.push_back(key_lens[i]);
        hx->p->puts.values.push_back(values[i]);
        hx->p->puts.value_lens.push_back(value_lens[i]);
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBPut
 * Add a BPUT into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bput
 * @param key_lens   the length of the keys to bput
 * @param values     the values associated with the keys
 * @param value_lens the length of the values
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBPut(hxhim_t *hx, void **keys, std::size_t *key_lens, void **values, std::size_t *value_lens, std::size_t num_keys) {
    return hxhim::BPut(hx, keys, key_lens, values, value_lens, num_keys);
}

/**
 * BGet
 * Add a BGET into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bget
 * @param key_lens   the length of the keys to bget
 * @return A wrapped return value containing responses
 */
int hxhim::BGet(hxhim_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys) {
    if (!hx || !hx->p || !keys || !key_lens) {
        return MDHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->gets.mutex);
    for(std::size_t i = 0; i < num_keys; i++) {
        hx->p->gets.keys.push_back(keys[i]);
        hx->p->gets.key_lens.push_back(key_lens[i]);
    }

    return MDHIM_SUCCESS;
}

/**
 * hxhimBGet
 * Add a BGET into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bget
 * @param key_lens   the length of the keys to bget
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGet(hxhim_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys) {
    return hxhim::BGet(hx, keys, key_lens, num_keys);
}

/**
 * BDelete
 * Add a BDEL into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bdel
 * @param key_lens   the length of the keys to bdelete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BDelete(hxhim_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys) {
    if (!hx || !hx->p || !keys || !key_lens) {
        return HXHIM_ERROR;
    }

    std::lock_guard<std::mutex> lock(hx->p->dels.mutex);
    for(std::size_t i = 0; i < num_keys; i++) {
        hx->p->dels.keys.push_back(keys[i]);
        hx->p->dels.key_lens.push_back(key_lens[i]);
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBDelete
 * Add a BDEL into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bdel
 * @param key_lens   the length of the keys to bdelete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBDelete(hxhim_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys) {
    return hxhim::BDelete(hx, keys, key_lens, num_keys);
}
