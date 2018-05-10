#include "bget.hpp"

/**
 * bget
 * Example usage and cleanup of mdhimBGet
 *
 * @param md       the mdhim context
 * @param keys     the keys
 * @param keys_len the length of each key
 * @param num_keys the number of keys
 * @param out      normal message stream
 * @param err      error message stream
 */
void bget(mdhim_t *md,
          void **keys, std::size_t *key_lens,
          std::size_t num_keys,
          std::ostream &out, std::ostream &err) {
    mdhim_bgetrm_t *bgrm = mdhimBGet(md, nullptr,
                                     keys, key_lens,
                                     num_keys,
                                     TransportGetMessageOp::GET_EQ);

    if (!bgrm) {
        err << "mdhimBGet error" << std::endl;
        return;
    }

    // Get and print results
    for(int ret = MDHIM_SUCCESS; (ret == MDHIM_SUCCESS) && bgrm; ret = next(&bgrm)) {
        // Get number of num_keys
        std::size_t get_num_keys = 0;
        if (mdhim_bgrm_num_keys(bgrm, &get_num_keys) != MDHIM_SUCCESS) {
            err << "Could not get number of num_keys" << std::endl;
            continue;
        }

        // Get keys
        void **get_keys = new void *[get_num_keys]();
        std::size_t *get_key_lens = new std::size_t[get_num_keys]();
        if (mdhim_bgrm_keys(bgrm, &get_keys, &get_key_lens) != MDHIM_SUCCESS) {
            err << "Could not get keys" << std::endl;
            delete [] get_keys;
            delete [] get_key_lens;
            continue;
        }

        // Get error value
        int error = MDHIM_ERROR;
        if (mdhim_bgrm_error(bgrm, &error) != MDHIM_SUCCESS) {
            err << "Could not BGET";
            for(std::size_t i = 0; i < num_keys; i++) {
                err << " " << std::string((char *)keys[i], key_lens[i]);
            }
            err << std::endl;
            delete [] get_keys;
            delete [] get_key_lens;
            continue;
        }

        // Check error value
        if (error == MDHIM_SUCCESS) {
            // Get values
            void **get_values = new void *[get_num_keys]();
            std::size_t *get_value_lens = new std::size_t[get_num_keys]();
            if (mdhim_bgrm_values(bgrm, &get_values, &get_value_lens) != MDHIM_SUCCESS) {
                err << "BGET error " << error << std::endl;
            }
            else {
                int src = -1;
                if (mdhim_bgrm_src(bgrm, &src) != MDHIM_SUCCESS) {
                    err << "Could not get return message source" << std::endl;
                    mdhim_bgrm_destroy(bgrm);
                    delete [] get_keys;
                    delete [] get_key_lens;
                    delete [] get_values;
                    delete [] get_value_lens;
                    return;
                }

                int *rs_idx = new int[get_num_keys]();
                if (mdhim_bgrm_rs_idx(bgrm, &rs_idx) != MDHIM_SUCCESS) {
                    err << "Could not get return message index" << std::endl;
                    mdhim_bgrm_destroy(bgrm);
                    delete [] rs_idx;
                    delete [] get_keys;
                    delete [] get_key_lens;
                    delete [] get_values;
                    delete [] get_value_lens;
                    return;
                }

                for(std::size_t i = 0; i < get_num_keys; i++) {
                    int db = -1;
                    if (mdhimComposeDB(md, &db, src, rs_idx[i]) != MDHIM_SUCCESS) {
                        err << "Could not compute database id" << std::endl;
                        mdhim_bgrm_destroy(bgrm);
                        delete [] rs_idx;
                        delete [] get_keys;
                        delete [] get_key_lens;
                        delete [] get_values;
                        delete [] get_value_lens;
                        return;
                    }

                    out << "BGET " << std::string((char *)get_keys[i], get_key_lens[i]) << " -> " << std::string((char *)get_values[i], get_value_lens[i]) << " from database " << db << std::endl;
                }

                delete [] rs_idx;
            }

            delete [] get_values;
            delete [] get_value_lens;
        }
        else {
            for(std::size_t i = 0; i < get_num_keys; i++) {
                out << "Could not BGET " << std::string((char *)get_keys[i], get_key_lens[i]) << " from database " << mdhimWhichDB(md, get_keys[i], get_key_lens[i]) << std::endl;
            }
        }

        delete [] get_keys;
        delete [] get_key_lens;
    }

    while (bgrm) {
        next(&bgrm);
    }
}
