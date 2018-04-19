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
          void **keys, int *key_lens,
          int num_keys,
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
        int num_keys = 0;
        if (mdhim_bgrm_num_keys(bgrm, &num_keys) != MDHIM_SUCCESS) {
            err << "Could not get number of num_keys" << std::endl;
            continue;
        }

        // Get keys
        void **bgrm_keys = nullptr;
        int *bgrm_key_lens = nullptr;
        if (mdhim_bgrm_keys(bgrm, &bgrm_keys, &bgrm_key_lens) != MDHIM_SUCCESS) {
            err << "Could not get keys" << std::endl;
            continue;
        }

        // Get error value
        int error = MDHIM_ERROR;
        if (mdhim_bgrm_error(bgrm, &error) != MDHIM_SUCCESS) {
            err << "Could not BGET";
            for(int i = 0; i < num_keys; i++) {
                err << " " << std::string((char *)keys[i], key_lens[i]);
            }
            err << std::endl;
            continue;
        }

        void **values = nullptr;
        int *value_lens = nullptr;

        // Check error value and get values
        if ((error == MDHIM_SUCCESS) &&
            (mdhim_bgrm_values(bgrm, &values, &value_lens) == MDHIM_SUCCESS)) {
            // Print key value pairs
            for(int i = 0; i < num_keys; i++) {
                out << "BGET " << std::string((char *)bgrm_keys[i], bgrm_key_lens[i]) << " -> " << std::string((char *)values[i], value_lens[i]) << " from range server on rank " << mdhimWhichServer(md, keys[i], key_lens[i]) << std::endl;
            }
        }
        else {
            // Print keys
            for(int i = 0; i < num_keys; i++) {
                out << "Could not BGET " << std::string((char *)bgrm_keys[i], bgrm_key_lens[i]) << " from range server on rank " << mdhimWhichServer(md, bgrm_keys[i], bgrm_key_lens[i]) << std::endl;
            }
            continue;
        }
    }

    while (bgrm) {
        next(&bgrm);
    }
}
