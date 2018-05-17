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
         void *primary_key, std::size_t primary_key_len,
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
        int src = -1;
        if (mdhim_rm_src(rm, &src) != MDHIM_SUCCESS) {
            err << "Could not get return message source" << std::endl;
            mdhim_rm_destroy(rm);
            return;
        }

        int rs_idx = -1;
        if (mdhim_rm_rs_idx(rm, &rs_idx) != MDHIM_SUCCESS) {
            err << "Could not get return message index" << std::endl;
            mdhim_rm_destroy(rm);
            return;
        }

        int db = -1;
        if (mdhimComposeDB(md, &db, src, rs_idx) != MDHIM_SUCCESS) {
            err << "Could not compute database id" << std::endl;
            mdhim_rm_destroy(rm);
            return;
        }

        out << "DEL " << std::string((char *)primary_key, primary_key_len) << " from database " << db << std::endl;
    }

    // destroying the return value must occur
    // after the private values are accessed
    mdhim_rm_destroy(rm);
}
