#include "get.hpp"

/**
 * get
 * Example usage and cleanup of mdhimGet
 *
 * @param md              the mdhim context
 * @param primary_key     the key
 * @param primary_key_len the length of the key
 * @param out             normal message stream
 * @param err             error message stream
 */
void get(mdhim_t *md,
         void *primary_key, std::size_t primary_key_len,
         std::ostream &out, std::ostream &err) {
    // Get the value
    mdhim_grm_t *grm = mdhimGet(md, nullptr,
                                primary_key, primary_key_len,
                                GET_EQ);

    if (!grm) {
        err << "mdhimGet error" << std::endl;
        return;
    }

    // Get error value
    int error = MDHIM_ERROR;
    if (mdhim_grm_error(grm, &error) != MDHIM_SUCCESS) {
        err << "Could not GET " << std::string((char *)primary_key, primary_key_len) << " from database " << mdhimWhichDB(md, primary_key, primary_key_len) << std::endl;
        mdhim_grm_destroy(grm);
        return;
    }

    // Check error value
    if (error != MDHIM_SUCCESS) {
        err << "GET " << std::string((char *)primary_key, primary_key_len) << " from database " << mdhimWhichDB(md, primary_key, primary_key_len) << " returned error " << error << std::endl;
        mdhim_grm_destroy(grm);
        return;
    }

    // Extract the keys from the returned value (do not free)
    char *key = nullptr;
    std::size_t key_len = 0;
    if (mdhim_grm_key(grm, (void **) &key, &key_len) != MDHIM_SUCCESS) {
        err << "Could not extract key" << std::endl;
        mdhim_grm_destroy(grm);
        return;
    }

    // Extract the values from the returned value (do not free)
    char *value = nullptr;
    std::size_t value_len = 0;
    if (mdhim_grm_value(grm, (void **) &value, &value_len) != MDHIM_SUCCESS) {
        err << "Could not extract value" << std::endl;
        mdhim_grm_destroy(grm);
        return;
    }

    int src = -1;
    if (mdhim_grm_src(grm, &src) != MDHIM_SUCCESS) {
        err << "Could not get return message source" << std::endl;
        mdhim_grm_destroy(grm);
        return;
    }

    int rs_idx = -1;
    if (mdhim_grm_rs_idx(grm, &rs_idx) != MDHIM_SUCCESS) {
        err << "Could not get return message index" << std::endl;
        mdhim_grm_destroy(grm);
        return;
    }

    int db = -1;
    if (mdhimComposeDB(md, &db, src, rs_idx) != MDHIM_SUCCESS) {
        err << "Could not compute database id" << std::endl;
        mdhim_grm_destroy(grm);
        return;
    }

    // Print value gotten back
    out << "GET " << std::string(key, key_len) << " -> " << std::string(value, value_len) << " from database " << db << std::endl;

    // destroying the return value must occur
    // after the private values are accessed
    mdhim_grm_destroy(grm);
}
