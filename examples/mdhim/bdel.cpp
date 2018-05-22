#include "bdel.hpp"

/**
 * bdel
 * Example usage and cleanup of mdhimBDelete
 *
 * @param md       the mdhim context
 * @param keys     the keys
 * @param keys_len the length of each key
 * @param num_keys the number of keys
 * @param out      normal message stream
 * @param err      error message stream
 */
void bdel(mdhim_t *md,
          void **keys, std::size_t *key_lens,
          std::size_t num_keys,
          std::ostream &out, std::ostream &err) {
    mdhim_brm_t *brm = mdhimBDelete(md, nullptr,
                                    keys, key_lens,
                                    num_keys);

    if (!brm) {
        err << "mdhimBGet error" << std::endl;
        return;
    }

    // Get and print results
    for(int ret = MDHIM_SUCCESS; (ret == MDHIM_SUCCESS) && brm; ret = next(&brm)) {
        // Get error value
        int error = MDHIM_ERROR;
        if (mdhim_brm_error(brm, &error) != MDHIM_SUCCESS) {
            err << "Could not BDELETE" << std::endl;
            mdhim_brm_destroy(brm);
            return;
        }

        // Check error value
        if (error != MDHIM_SUCCESS) {
            err << "BDEL error " << error << std::endl;
        }
    }

    if (!brm) {
        for(std::size_t i = 0; i < num_keys; i++) {
            out << "BDEL " << std::string((char *)keys[i], key_lens[i]) << " from database " << mdhimWhichDB(md, keys[i], key_lens[i]) << std::endl;;
        }
    }

    while (brm) {
        next(&brm);
    }
}
