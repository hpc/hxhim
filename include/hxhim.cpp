#include "hxhim.hpp"
#include "hxhim_private.hpp"

namespace hxhim {

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
 * open
 * Start a HXHIM session
 *
 * @param hx             the HXHIM session
 * @param bootstrap_comm the MPI communicator used to boostrap MDHIM
 * @param filename       the name of the file to open (for now, it is only the mdhim configuration)
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 *
 */
int open(hxhim_session_t *hx, const MPI_Comm bootstrap_comm, const std::string &filename) {
    if (!hx) {
        return HXHIM_ERROR;
    }

    if (!(hx->p = new hxhim_session_private_t())) {
        return HXHIM_ERROR;
    }

    // initialize options through config
    const std::string mdhim_config = filename; // the mdhim_config value should be part of the hxhim configuration
    if (!(hx->p->mdhim_opts = new mdhim_options_t())                                            ||
        (config_reader(hx->p->mdhim_opts, bootstrap_comm, mdhim_config) != MDHIM_SUCCESS)) {
        close(hx);
        return HXHIM_ERROR;
    }

    // initialize mdhim context
    if (!(hx->p->md = new mdhim_t())                               ||
        (mdhim::Init(hx->p->md, hx->p->mdhim_opts) != MDHIM_SUCCESS)) {
        close(hx);
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * close
 * Terminates a HXHIM session
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int close(hxhim_session_t *hx) {
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    // clear out unsent work in the work queue
    for(work_t *work : hx->p->queue) {
        delete work;
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

    // clean up pointer to private data
    delete hx->p;
    hx->p = nullptr;

    return HXHIM_SUCCESS;
}

/**
 * flush
 * Push all queued work into MDHIM
 *
 * @param hx
 * @return HXHIM_SUCCESS or HXHIM_ERROR if any error occured
 */
Return *flush(hxhim_session_t *hx) {
    std::lock_guard<std::mutex> lock(hx->p->queue_mutex);

    // process all of the work
    Return *ret = new Return(work_t::Op::NONE, false, nullptr);
    Return *head = ret;
    while (hx->p->queue.size()) {
        hxhim::work_t *work = hx->p->queue.front();
        hx->p->queue.pop_front();

        // empty work
        if (!work) {
            continue;
        }

        // make sure keys and lengths match up
        if (work->keys.size() != work->key_lens.size()) {
            mlog(MLOG_WARN, "HXHIM Rank %d - Attempted to flush message with mismatched key (%zu) and key length data %zu. Skipping.", hx->p->md->rank, work->keys.size(), work->key_lens.size());
            delete work;
            ret = ret->Next(new Return(work->op, false, nullptr));
            continue;
        }

        // when there is only 1 set of data to operate on, use single PUT/GET/DEL
        if (work->keys.size() == 1) {
            switch (work->op) {
                case work_t::Op::PUT:
                    {
                        put_work_t *put = dynamic_cast<put_work_t *>(work);
                        TransportRecvMessage *rm = mdhim::Put(hx->p->md, nullptr, put->keys[0], put->key_lens[0], put->values[0], put->value_lens[0]);
                        ret = ret->Next(new Return(work->op, true, rm));
                    }
                    break;
                case work_t::Op::GET:
                    {
                        get_work_t *get = dynamic_cast<get_work_t *>(work);
                        TransportGetRecvMessage *grm = mdhim::Get(hx->p->md, nullptr, get->keys[0], get->key_lens[0], get->get_op);
                        ret = ret->Next(new GetReturn(work->op, grm));
                    }
                    break;
                case work_t::Op::DEL:
                    {
                        del_work_t *del = dynamic_cast<del_work_t *>(work);
                        TransportRecvMessage *rm = mdhim::Delete(hx->p->md, nullptr, del->keys[0], del->key_lens[0]);
                        ret = ret->Next(new Return(work->op, true, rm));
                    }
                    break;
                case work_t::Op::NONE:
                default:
                    break;
            }
        }
        // use BPUT/BGET/BDEL
        else {
            // copy the keys and lengths into arrays
            void **keys = new void *[work->keys.size()]();
            std::size_t *key_lens = new std::size_t[work->key_lens.size()]();
            for(std::size_t i = 0; i < work->keys.size(); i++) {
                keys[i] = work->keys[i];
                key_lens[i] = work->key_lens[i];
            }

            if (!keys || !key_lens) {
                delete [] keys;
                delete [] key_lens;
                delete work;
                ret = ret->Next(new Return(work->op, false, nullptr));
                continue;
            }

            // can add some async stuff here, if keys are sorted here instead of in MDHIM
            switch (work->op) {
                case work_t::Op::PUT:
                    {
                        put_work_t *put = dynamic_cast<put_work_t *>(work);
                        if (put->values.size() != put->value_lens.size()) {
                            mlog(MLOG_WARN, "HXHIM Rank %d - Attempted to flush message with mismatched value (%zu) and value length data %zu. Skipping.", hx->p->md->rank, put->values.size(), put->value_lens.size());
                            delete work;
                            ret = ret->Next(new Return(work->op, false, nullptr));
                            continue;
                        }

                        // copy the values and lengths into arrays
                        void **values = new void *[put->values.size()]();
                        std::size_t *value_lens = new std::size_t[put->value_lens.size()]();
                        if (!values || !value_lens) {
                            delete [] values;
                            delete [] value_lens;
                            delete work;
                            ret = ret->Next(new Return(work->op, false, nullptr));
                            continue;
                        }

                        for(std::size_t i = 0; i < put->values.size(); i++) {
                            values[i] = put->values[i];
                            value_lens[i] = put->value_lens[i];
                        }

                        TransportBRecvMessage *brm = mdhim::BPut(hx->p->md, nullptr, keys, key_lens, values, value_lens, put->keys.size());
                        ret = ret->Next(new Return(work->op, true, brm));

                        delete [] values;
                        delete [] value_lens;
                    }
                    break;
                case work_t::Op::GET:
                    {
                        get_work_t *get = dynamic_cast<get_work_t *>(work);
                        TransportBGetRecvMessage *bgrm = mdhim::BGet(hx->p->md, nullptr, keys, key_lens, get->keys.size(), get->get_op);
                        ret = ret->Next(new GetReturn(work->op, bgrm));
                    }
                    break;
                case work_t::Op::DEL:
                    {
                        del_work_t *del = dynamic_cast<del_work_t *>(work);
                        TransportBRecvMessage *brm = mdhim::BDelete(hx->p->md, nullptr, keys, key_lens, del->keys.size());
                        ret = ret->Next(new Return(work->op, true, brm));
                    }
                    break;
                case work_t::Op::NONE:
                default:
                    break;
            }

            delete [] keys;
            delete [] key_lens;
        }

        delete work;
    }

    ret = head->Next();
    delete head;

    return ret;
}

/**
 * put
 * Add a PUT into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to put
 * @param key_len   the length of the key to put
 * @param value     the value associated with the key
 * @param value_len the length of the value
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int put(hxhim_session_t *hx, void *key, std::size_t key_len, void *value, std::size_t value_len) {
    if (!hx || !hx->p || !key || !value) {
        return HXHIM_ERROR;
    }

    put_work_t *work = dynamic_cast<put_work_t *>(get_matching_work(hx, work_t::Op::PUT));
    if (!work) {
        return HXHIM_ERROR;
    }

    work->keys.push_back(key);
    work->key_lens.push_back(key_len);

    work->values.push_back(value);
    work->value_lens.push_back(value_len);

    return HXHIM_SUCCESS;
}

/**
 * get
 * Add a GET into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to get
 * @param key_len   the length of the key to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int get(hxhim_session_t *hx, void *key, std::size_t key_len) {
    if (!hx || !hx->p || !key) {
        return HXHIM_ERROR;
    }

    get_work_t *work = dynamic_cast<get_work_t *>(get_matching_work(hx, work_t::Op::GET));
    if (!work) {
        return HXHIM_ERROR;
    }

    work->keys.push_back(key);
    work->key_lens.push_back(key_len);

    return HXHIM_SUCCESS;
}

/**
 * del
 * Add a DEL into the work queue
 *
 * @param hx        the HXHIM session
 * @param key       the key to del
 * @param key_len   the length of the key to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int del(hxhim_session_t *hx, void *key, std::size_t key_len) {
    if (!hx || !hx->p || !key) {
        return HXHIM_ERROR;
    }

    del_work_t *work = dynamic_cast<del_work_t *>(get_matching_work(hx, work_t::Op::DEL));
    if (!work) {
        return HXHIM_ERROR;
    }

    work->keys.push_back(key);
    work->key_lens.push_back(key_len);

    return HXHIM_SUCCESS;
}

/**
 * bput
 * Add a BPUT into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bput
 * @param key_lesn   the length of the keys to bput
 * @param values     the values associated with the keys
 * @param value_lens the length of the values
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int bput(hxhim_session_t *hx, void **keys, std::size_t *key_lens, void **values, std::size_t *value_lens, std::size_t num_keys) {
    if (!hx || !hx->p || !keys || !key_lens || !values || !value_lens) {
        return HXHIM_ERROR;
    }

    put_work_t *work = dynamic_cast<put_work_t *>(get_matching_work(hx, work_t::Op::PUT));
    if (!work) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < num_keys; i++) {
        work->keys.push_back(keys[i]);
        work->key_lens.push_back(key_lens[i]);

        work->values.push_back(values[i]);
        work->value_lens.push_back(value_lens[i]);
    }

    return HXHIM_SUCCESS;
}

/**
 * bget
 * Add a BGET into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bget
 * @param key_lesn   the length of the keys to bget
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int bget(hxhim_session_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys) {
    if (!hx || !hx->p || !keys || !key_lens) {
        return HXHIM_ERROR;
    }

    get_work_t *work = dynamic_cast<get_work_t *>(get_matching_work(hx, work_t::Op::GET));
    if (!work) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < num_keys; i++) {
        work->keys.push_back(keys[i]);
        work->key_lens.push_back(key_lens[i]);
    }

    return HXHIM_SUCCESS;
}

/**
 * bdel
 * Add a BDEL into the work queue
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bdel
 * @param key_lesn   the length of the keys to bdelete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int bdel(hxhim_session_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys) {
    if (!hx || !hx->p || !keys || !key_lens) {
        return HXHIM_ERROR;
    }

    del_work_t *work = dynamic_cast<del_work_t *>(get_matching_work(hx, work_t::Op::DEL));
    if (!work) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < num_keys; i++) {
        work->keys.push_back(keys[i]);
        work->key_lens.push_back(key_lens[i]);
    }

    return HXHIM_SUCCESS;
}

}
