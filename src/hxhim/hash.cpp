#include "hxhim/hash.hpp"

hxhim_t *hxhim::hash::hx_ = nullptr;

void hxhim::hash::init(hxhim_t *hx) {
    hx_ = hx;
}

void hxhim::hash::destroy() {
    hx_ = nullptr;
}

/**
 * Rank
 * Returns the rank of the process as the target database
 *
 * @param data     the byte string to hash
 * @param data_len the length of the byte string
 * @param args     a pointer to an int that contains the rank of the calling function
 * @return the destination database ID or -1 on error
 */
int hxhim::hash::Rank(void *subject, const std::size_t subject_len, void *predicate, const std::size_t predicate_len, void *args) {
    int *rank = static_cast<int *>(args);
    (void) subject;
    (void) subject_len;
    (void) predicate;
    (void) predicate_len;
    return rank?*rank:-1;
}

/**
 * SumModSDatabases
 * Simple hash that sums the bytes of the data
 * and mods it by the number of databases.
 *
 * @param data     the byte string to hash
 * @param data_len the length of the byte string
 * @param args     a pointer to an int that contains the number of databases there are
 * @return the destination database ID or -1 on error
 */
int hxhim::hash::SumModDatabases(void *subject, const std::size_t subject_len, void *predicate, const std::size_t predicate_len, void *args) {
    int *databases = static_cast<int *>(args);
    if (!databases || (*databases < 0)) {
        return -1;
    }

    static const int mod = *databases * hx_->mpi.size;

    int dst = 0;
    for(std::size_t i = 0; i < subject_len; i++) {
        dst += (uint8_t) ((char *) subject)[i];
    }

    for(std::size_t i = 0; i < predicate_len; i++) {
        dst += (uint8_t) ((char *) predicate)[i];
    }

    return dst % mod;
}
