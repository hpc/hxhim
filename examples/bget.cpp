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
        int get_num_keys = 0;
        if (mdhim_bgrm_num_keys(bgrm, &get_num_keys) != MDHIM_SUCCESS) {
            err << "Could not get number of num_keys" << std::endl;
            continue;
        }

        // Get keys
        void **get_keys = nullptr;
        int *get_key_lens = nullptr;
        if (mdhim_bgrm_keys(bgrm, &get_keys, &get_key_lens) != MDHIM_SUCCESS) {
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

        // Check error value
        if (error == MDHIM_SUCCESS) {
            // Get values
            void **get_values = nullptr;
            int *get_value_lens = nullptr;
            if (mdhim_bgrm_values(bgrm, &get_values, &get_value_lens) != MDHIM_SUCCESS) {
                err << "BGET error " << error << std::endl;
            }
            else {
                for(int i = 0; i < get_num_keys; i++) {
                    int src;
                    out << "BGET " << std::string((char *)get_keys[i], get_key_lens[i]) << " -> " << std::string((char *)get_values[i], get_value_lens[i]) << " from range server on rank " << ((mdhim_bgrm_src(bgrm, &src) == MDHIM_SUCCESS)?src:-1) << std::endl;
                }
            }
        }
        else {
            for(int i = 0; i < get_num_keys; i++) {
                int src;
                out << "Could not BGET " << std::string((char *)get_keys[i], get_key_lens[i]) << " from range server on rank " << ((mdhim_bgrm_src(bgrm, &src) == MDHIM_SUCCESS)?src:-1) << std::endl;
            }
        }
    }

    while (bgrm) {
        next(&bgrm);
    }
}
