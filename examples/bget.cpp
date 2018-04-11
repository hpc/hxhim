#include "bget.hpp"

/**
 * bget
 * Example usage and cleanup of mdhimbget
 *
 * @param md       the mdhim context
 * @param keys     the keys
 * @param keys_len the length of each key
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

    // Get and print results
    while (bgrm) {
        // Get number of num_keys
        int num_keys = 0;
        if (mdhim_bgrm_num_keys(bgrm, &num_keys) != MDHIM_SUCCESS) {
            err << "Could not get number of num_keys" << std::endl;
            mdhim_bgrm_destroy(bgrm);
            return;
        }

        // Get keys
        void **keys = nullptr;
        int *key_lens = nullptr;
        if (mdhim_bgrm_keys(bgrm, &keys, &key_lens) != MDHIM_SUCCESS) {
            err << "Could not get keys" << std::endl;
            mdhim_bgrm_destroy(bgrm);
            return;
        }

        // Get error value
        int error = MDHIM_ERROR;
        if (mdhim_bgrm_error(bgrm, &error) != MDHIM_SUCCESS) {
            err << "Could not BGET";
            for(int i = 0; i < num_keys; i++) {
                err << " " << std::string((char *)keys[i], key_lens[i]);
            }
            err << std::endl;
            mdhim_bgrm_destroy(bgrm);
            return;
        }

        // Check error value
        if (error == MDHIM_SUCCESS) {
            // Get values
            void **values = nullptr;
            int *value_lens = nullptr;
            if (mdhim_bgrm_values(bgrm, &values, &value_lens) != MDHIM_SUCCESS) {
                err << "BGET error " << error << std::endl;
                mdhim_bgrm_destroy(bgrm);
                return;
            }

            // Print key value pairs
            for(int i = 0; i < num_keys; i++) {
                out << "BGET " << std::string((char *)keys[i], key_lens[i]) << " -> " << std::string((char *)values[i], value_lens[i]) << " from range server on rank " << mdhimWhichServer(md, keys[i], key_lens[i]) << std::endl;
            }
        }
        else {
            err << "BGET error" << std::endl;
        }

        // Go to next result
        mdhim_bgetrm_t *next = nullptr;
        if (mdhim_bgrm_next(bgrm, &next) != MDHIM_SUCCESS) {
            break;
        }

        mdhim_bgrm_destroy(bgrm);
        bgrm = next;
    }
}
