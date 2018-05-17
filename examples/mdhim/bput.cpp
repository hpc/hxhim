#include "bput.hpp"

/**
 * bput
 * Example usage and cleanup of mdhimBPut
 *
 * @param md               the mdhim context
 * @param primary_keys     the keys
 * @param primary_key_lens the length of each key
 * @param value            the values indexed by the keys
 * @param value_len        the length of each value
 * @param num_keys         the number of key value pairs
 * @param out              normal message stream
 * @param err              error message stream
 */
void bput(mdhim_t *md,
          void **primary_keys, std::size_t *primary_key_lens,
          void **values, std::size_t *value_lens,
          std::size_t num_keys,
          std::ostream &out, std::ostream &err) {
    mdhim_brm_t *brm = mdhimBPut(md, nullptr,
                                 primary_keys, primary_key_lens,
                                 values, value_lens,
                                 num_keys);

    if (!brm) {
        err << "mdhimBPut error" << std::endl;
        return;
    }

    // Get and print results
    for(int ret = MDHIM_SUCCESS; (ret == MDHIM_SUCCESS) && brm; ret = next(&brm)) {
        int src = -1;
        if (mdhim_brm_src(brm, &src) != MDHIM_SUCCESS) {
            err << "Could not get return message source" << std::endl;
            continue;
        }

        // Get error value
        int error = MDHIM_ERROR;
        if (mdhim_brm_error(brm, &error) != MDHIM_SUCCESS) {
            err << "Could not get error" << std::endl;
            continue;
        }

        // Check error value
        if (error != MDHIM_SUCCESS) {
            err << "BPUT to range server " << src << " returned error " << error << std::endl;
            continue;
        }

        std::size_t num_keys_to_rs = 0;
        if (mdhim_brm_num_keys(brm, &num_keys_to_rs) != MDHIM_SUCCESS) {
            err << "Could not get number of keys sent to range server " << src << std::endl;
            continue;
        }

        int *rs_idx = new int[num_keys_to_rs]();
        if (mdhim_brm_rs_idx(brm, &rs_idx) != MDHIM_SUCCESS) {
            err << "Could not get return message indicies" << std::endl;
            continue;
        }

        for(std::size_t i = 0; i < num_keys_to_rs; i++) {
            int db = -1;
            if (mdhimComposeDB(md, &db, src, rs_idx[i]) != MDHIM_SUCCESS) {
                err << "Could not compute database id" << std::endl;
                continue;
            }

            out << "BPUT to database " << db  << " succeeded" << std::endl;
        }

        delete [] rs_idx;
    }
}
