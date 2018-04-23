#include "del.hpp"

/**
 * del
 * Example usage and cleanup of mdhimDelete
 *
 * @param md              the mdhim context
 * @param primary_key     the key
 * @param primary_key_len the length of the key
 * @param out             normal message stream
 * @param err             error message stream
 */
void del(mdhim_t *md,
         void *primary_key, int primary_key_len,
         std::ostream &out, std::ostream &err) {
    // Del the value
    mdhim_rm_t *rm = mdhimDelete(md, nullptr,
                                 primary_key, primary_key_len);

    if (!rm) {
        err << "mdhimDel error" << std::endl;
        return;
    }

    // Get error value
    int error = MDHIM_ERROR;
    if (mdhim_rm_error(rm, &error) != MDHIM_SUCCESS) {
        err << "Could not DELETE" << std::endl;
        mdhim_rm_destroy(rm);
        return;
    }

    // Check error value
    if (error != MDHIM_SUCCESS) {
        err << "DEL error " << error << std::endl;
    }
    else {
        int src;
        out << "DEL " << std::string((char *)primary_key, primary_key_len) << " from range server on rank " << ((mdhim_rm_src(rm, &src) == MDHIM_SUCCESS)?src:-1) << std::endl;
    }

    // destroying the return value must occur
    // after the private values are accessed
    mdhim_rm_destroy(rm);
}
