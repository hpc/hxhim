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
 * @param out              normal message stream
 * @param err              error message stream
 */
void bput(mdhim_t *md,
          void **primary_keys, int *primary_key_lens,
          void **values, int *value_lens,
          int num_keys,
          std::ostream &out, std::ostream &err) {
    mdhim_brm_t *brm = mdhimBPut(md,
                                 primary_keys, primary_key_lens,
                                 values, value_lens,
                                 num_keys,
                                 nullptr,
                                 nullptr);

    if (!brm) {
        err << "mdhimBPut error" << std::endl;
        return;
    }

    // Get and print results
    while (brm) {
        // Get error value
        int error = MDHIM_ERROR;
        if (mdhim_brm_error(brm, &error) != MDHIM_SUCCESS) {
            err << "Could not BPUT" << std::endl;
            mdhim_brm_destroy(brm);
            return;
        }

        // Check error value
        if (error != MDHIM_SUCCESS) {
            err << "BPUT error " << error << std::endl;
        }

        // Get next result
        mdhim_brm_t *next = nullptr;
        if (mdhim_brm_next(brm, &next) != MDHIM_SUCCESS) {
            mdhim_brm_destroy(brm);
            break;
        }

        // destroy the current node
        mdhim_brm_destroy(brm);
        brm = next;
    }

    if (!brm) {
        for(int i = 0; i < num_keys; i++) {
            out << "BPUT " << std::string((char *)primary_keys[i], primary_key_lens[i]) << " -> " << std::string((char *)values[i], value_lens[i]) << " to range server on rank " << mdhimWhichServer(md, primary_keys[i], primary_key_lens[i]) << std::endl;
        }
    }

    mdhim_brm_destroy(brm);
}
