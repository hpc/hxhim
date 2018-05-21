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
 * Flush
 * Push the PUT stream so that the keys are visible.
 *
 * @param hx
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Flush(hxhim_t *hx) {
    hxhim::put_stream_t &puts = hx->p->puts;
    std::lock_guard<std::mutex> lock(puts.mutex);

    // make sure keys and values match up
    if ((puts.keys.size()   != puts.key_lens.size())   ||
        (puts.values.size() != puts.value_lens.size()) ||
        (puts.keys.size()   != puts.values.size()))     {
        mlog(MLOG_WARN, "HXHIM Rank %d - Attempted to flush message with mismatched lengths: key (%zu), key length (%zu), value (%zu), value length (%zu). Skipping.", hx->p->md->rank, puts.keys.size(), puts.key_lens.size(), puts.values.size(), puts.value_lens.size());
        puts.clear();
        return HXHIM_ERROR;
    }

    int ret = HXHIM_SUCCESS;

    // when there is only 1 set of data to operate on, use single PUT
    if (puts.keys.size() == 1) {
        TransportRecvMessage *rm = mdhim::Put(hx->p->md, nullptr, puts.keys[0], puts.key_lens[0], puts.values[0], puts.value_lens[0]);
        ret = (rm && (rm->error == MDHIM_SUCCESS))?HXHIM_SUCCESS:HXHIM_ERROR;
        delete rm;
    }
    // use BPUT
    else {
        // copy the keys and lengths into arrays
        void **keys = new void *[puts.keys.size()]();
        std::size_t *key_lens = new std::size_t[puts.key_lens.size()]();
        void **values = new void *[puts.values.size()]();
        std::size_t *value_lens = new std::size_t[puts.value_lens.size()]();

        if (!keys   || !key_lens   ||
            !values || !value_lens) {
            delete [] keys;
            delete [] key_lens;
            delete [] values;
            delete [] value_lens;
            return HXHIM_ERROR;
        }

        for(std::size_t i = 0; i < puts.keys.size(); i++) {
            keys[i] = puts.keys[i];
            key_lens[i] = puts.key_lens[i];
            values[i] = puts.values[i];
            value_lens[i] = puts.value_lens[i];
        }

        // can add some async stuff here, if keys are sorted here instead of in MDHIM

        TransportBRecvMessage *brm = mdhim::BPut(hx->p->md, nullptr, keys, key_lens, values, value_lens, puts.keys.size());

        // check for errors
        while (brm) {
            if (brm->error != MDHIM_SUCCESS) {
                ret = HXHIM_ERROR;
            }

            TransportBRecvMessage *next = brm->next;
            delete brm;
            brm = next;
        }

        // cleanup
        delete [] keys;
        delete [] key_lens;
        delete [] values;
        delete [] value_lens;

        puts.clear();
    }

    return ret;
}

/**
 * hxhimFlush
 * Push all queued work into MDHIM
 *
 * @param hx
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimFlush(hxhim_t *hx) {
    return hxhim::Flush(hx);
}

/**
 * FlushGet
 * Flush the queued PUTs before getting
 *
 * @param hx
 * @param key       the key to get
 * @param key_len   the length of the key to get
 * @return A wrapped return value containing responses
 */
hxhim::Return *hxhim::FlushGet(hxhim_t *hx, void *key, size_t key_len) {
    return (hxhim::Flush(hx) == HXHIM_SUCCESS)?hxhim::Get(hx, key, key_len):nullptr;
}

/**
 * hxhimFlushGet
 * Flush the queued PUTs before getting
 *
 * @param hx
 * @param key       the key to get
 * @param key_len   the length of the key to get
 * @return A wrapped return value containing responses
 */
hxhim_return_t *hxhimFlushGet(hxhim_t *hx, void *key, size_t key_len) {
    return hxhim_return_init(hxhim::FlushGet(hx, key, key_len));
}

/**
 * FlushBGet
 * Flush the queued PUTs before getting
 *
 * @param hx
 * @param keys       the keys to bget
 * @param key_lens   the length of the keys to bget
 * @return A wrapped return value containing responses
 */
hxhim::Return *hxhim::FlushBGet(hxhim_t *hx, void **keys, size_t *key_lens, std::size_t num_keys) {
    return (hxhim::Flush(hx) == HXHIM_SUCCESS)?hxhim::BGet(hx, keys, key_lens, num_keys):nullptr;
}

/**
 * hxhimFlushBGet
 * Flush the queued PUTs before getting
 *
 * @param hx
 * @param keys       the keys to bget
 * @param key_lens   the length of the keys to bget
 * @return A wrapped return value containing responses
 */
hxhim_return_t *hxhimFlushBGet(hxhim_t *hx, void **keys, size_t *key_lens, std::size_t num_keys) {
    return hxhim_return_init(hxhim::FlushBGet(hx, keys, key_lens, num_keys));
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
 * @return A wrapped return value containing responses
 */
hxhim::Return *hxhim::Get(hxhim_t *hx, void *key, std::size_t key_len) {
    if (!hx || !hx->p || !key) {
        return nullptr;
    }

    return new hxhim::Return(HXHIM_GET, mdhim::Get(hx->p->md, nullptr, key, key_len, GET_EQ));
}

/**
 * hxhimGet
 * Add a GET into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to get
 * @param key_len   the length of the key to get
 * @return A wrapped return value containing responses
 */
hxhim_return_t *hxhimGet(hxhim_t *hx, void *key, std::size_t key_len) {
    return hxhim_return_init(hxhim::Get(hx, key, key_len));
}

// /**
//  * Delete
//  * Add a DEL into the work queue
//  *
//  * @param hx        the HXHIM session
//  * @param key       the key to del
//  * @param key_len   the length of the key to delete
//  * @return HXHIM_SUCCESS or HXHIM_ERROR
//  */
// int hxhim::Delete(hxhim_t *hx, void *key, std::size_t key_len) {
//     if (!hx || !hx->p || !key) {
//         return HXHIM_ERROR;
//     }

//     del_work_t *work = dynamic_cast<del_work_t *>(get_matching_work(hx, hxhim_work_op::HXHIM_DEL));
//     if (!work) {
//         return HXHIM_ERROR;
//     }

//     puts.keys.push_back(key);
//     puts.key_lens.push_back(key_len);

//     return HXHIM_SUCCESS;
// }

// /**
//  * hxhimDelete
//  * Add a DEL into the work queue
//  *
//  * @param hx        the HXHIM session
//  * @param key       the key to del
//  * @param key_len   the length of the key to delete
//  * @return HXHIM_SUCCESS or HXHIM_ERROR
//  */
// int hxhimDelete(hxhim_t *hx, void *key, std::size_t key_len) {
//     return hxhim::Delete(hx, key, key_len);
// }

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
hxhim::Return *hxhim::BGet(hxhim_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys) {
    if (!hx || !hx->p || !keys || !key_lens) {
        return nullptr;
    }

    return new hxhim::Return(HXHIM_GET, mdhim::BGet(hx->p->md, nullptr, keys, key_lens, num_keys, GET_EQ));
}

/**
 * hxhimBGet
 * Add a BGET into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bget
 * @param key_lens   the length of the keys to bget
 * @return A wrapped return value containing responses
 */
hxhim_return_t *hxhimBGet(hxhim_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys) {
    return hxhim_return_init(hxhim::BGet(hx, keys, key_lens, num_keys));
}

// /**
//  * BDelete
//  * Add a BDEL into the work queue
//  *
//  * @param hx         the HXHIM session
//  * @param keys       the keys to bdel
//  * @param key_lens   the length of the keys to bdelete
//  * @return HXHIM_SUCCESS or HXHIM_ERROR
//  */
// int hxhim::BDelete(hxhim_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys) {
//     if (!hx || !hx->p || !keys || !key_lens) {
//         return HXHIM_ERROR;
//     }

//     del_work_t *work = dynamic_cast<del_work_t *>(get_matching_work(hx, hxhim_work_op::HXHIM_DEL));
//     if (!work) {
//         return HXHIM_ERROR;
//     }

//     for(std::size_t i = 0; i < num_keys; i++) {
//         puts.keys.push_back(keys[i]);
//         puts.key_lens.push_back(key_lens[i]);
//     }

//     return HXHIM_SUCCESS;
// }

// /**
//  * hxhimBDelete
//  * Add a BDEL into the work queue
//  *
//  * @param hx         the HXHIM session
//  * @param keys       the keys to bdel
//  * @param key_lens   the length of the keys to bdelete
//  * @return HXHIM_SUCCESS or HXHIM_ERROR
//  */
// int hxhimBDelete(hxhim_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys) {
//     return hxhim::BDelete(hx, keys, key_lens, num_keys);
// }
