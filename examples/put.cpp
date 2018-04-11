#include "put.hpp"

/**
 * put
 * Example usage and cleanup of mdhimPut
 *
 * @param md              the mdhim context
 * @param primary_key     the key
 * @param primary_key_len the length of the key
 * @param value           the value indexed by the key
 * @param value_len       the length of the value
 * @param out             normal message stream
 * @param err             error message stream
 */
void put(mdhim_t *md,
         void *primary_key, int primary_key_len,
         void *value, int value_len,
         std::ostream &out, std::ostream &err) {
    // Put the key-value pair
    mdhim_brm_t *brm = mdhimPut(md,
                                primary_key, primary_key_len,
                                value, value_len,
                                nullptr, nullptr);

    if (!brm) {
        err << "mdhimPut error" << std::endl;
        return;
    }

    // Get error value
    int error = MDHIM_ERROR;
    if (mdhim_brm_error(brm, &error) != MDHIM_SUCCESS) {
        err << "Could not PUT" << std::endl;
        mdhim_brm_destroy(brm);
        return;
    }

    // Check error value
    if (error != MDHIM_SUCCESS) {
        err << "PUT error " << error << std::endl;
    }
    else {
        out << "PUT " << std::string((char *)primary_key, primary_key_len) << " -> " << std::string((char *)value, value_len) << " to range server on rank " << mdhimWhichServer(md, primary_key, primary_key_len) << std::endl;
    }

    // destroying the return value must occur
    // after the private values are accessed
    mdhim_brm_destroy(brm);
}
