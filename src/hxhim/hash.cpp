#include "hxhim/accessors.hpp"
#include "hxhim/hash.hpp"
#include "hxhim/private.hpp"

/**
 * Rank
 * Returns the rank of the process as the target datastore
 *
 * @param data     the byte string to hash
 * @param data_len the length of the byte string
 * @param args     a pointer to an int that contains the rank of the calling function
 * @return the destination datastore ID or -1 on error
 */
int hxhim::hash::Rank(hxhim_t *hx, void *subject, const std::size_t subject_len, void *predicate, const std::size_t predicate_len, void *args) {
    (void) subject;
    (void) subject_len;
    (void) predicate;
    (void) predicate_len;
    return (hx && hx->p)?hx->p->bootstrap.rank:-1;
}

/**
 * SumModSDatastores
 * Simple hash that sums the bytes of the data
 * and mods it by the number of datastores.
 *
 * @param data     the byte string to hash
 * @param data_len the length of the byte string
 * @param args     a pointer to an int that contains the number of datastores there are
 * @return the destination datastore ID or -1 on error
 */
int hxhim::hash::SumModDatastores(hxhim_t *hx, void *subject, const std::size_t subject_len, void *predicate, const std::size_t predicate_len, void *args) {
    if (!hx || !hx->p) {
        return -1;
    }

    static const int mod = hx->p->datastore.count * hx->p->bootstrap.size;

    int dst = 0;
    for(std::size_t i = 0; i < subject_len; i++) {
        dst += (uint8_t) ((char *) subject)[i];
    }

    for(std::size_t i = 0; i < predicate_len; i++) {
        dst += (uint8_t) ((char *) predicate)[i];
    }

    return dst % mod;
}
