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
        int src;
        if (mdhim_brm_src(brm, &src) != MDHIM_SUCCESS) {
            err << "Could not get source from BPUT response message" << std::endl;
            continue;
        }

        // Get error value
        int error = MDHIM_ERROR;
        if (mdhim_brm_error(brm, &error) != MDHIM_SUCCESS) {
            err << "Could not BPUT to " << src << brm << std::endl;
            continue;
        }

        // Check error value
        if (error != MDHIM_SUCCESS) {
            err << "BPUT to " << src  << " returned error " << error << std::endl;
            continue;
        }

        out << "BPUT to " << src  << " succeeded" << std::endl;
    }
}
