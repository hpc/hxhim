#include <cmath>

#include "hxhim/hash.hpp"
#include "hxhim/private.hpp"

static std::size_t num_datastores(hxhim_t *hx) {
    // no need to check if hx is valid here
    static const std::size_t datastores = hx->p->range_server.server_ratio * ((hx->p->bootstrap.size / hx->p->range_server.client_ratio)) + std::min(hx->p->bootstrap.size % hx->p->range_server.client_ratio, hx->p->range_server.server_ratio);

    return datastores;
}

/**
 * RankZero
 *
 * @return 0
 */
int hxhim::hash::RankZero(hxhim_t *, void *, const std::size_t, void *, const std::size_t, void *) {
    return 0;
}

/**
 * MyRank
 * Returns the rank of the process as the target datastore
 *
 * @param hx            the HXHIM instance
 * @return the destination datastore ID or -1 on error
 */
int hxhim::hash::MyRank(hxhim_t *hx, void *, const std::size_t, void *, const std::size_t, void *) {
    if (!hxhim::valid(hx)) {
        return -1;
    }

    if (hx->p->range_server.server_ratio != hx->p->range_server.client_ratio) {
        return -1;
    }

    return hx->p->bootstrap.rank;
}

/**
 * RankModDatastores
 * Returns the rank % datastores of the process as the target datastore
 *
 * @param hx            the HXHIM instance
 * @return the destination datastore ID or -1 on error
 */
int hxhim::hash::RankModDatastores(hxhim_t *hx, void *, const std::size_t, void *, const std::size_t, void *) {
    if (!hxhim::valid(hx)) {
        return -1;
    }

    return hx->p->bootstrap.rank % num_datastores(hx);;
}

/**
 * SumModDatastores
 * Simple hash that sums the bytes of the data
 * and mods it by the number of datastores.
 *
 * @param hx            the HXHIM instance
 * @param subject       the subject to hash
 * @param subject_len   the length of the subject
 * @param predicate     the predicate to hash
 * @param predicate_len the length of the predicate
 * @return the destination datastore ID or -1 on error
 */
int hxhim::hash::SumModDatastores(hxhim_t *hx, void *subject, const std::size_t subject_len, void *predicate, const std::size_t predicate_len, void *) {
    if (!hxhim::valid(hx)) {
        return -1;
    }

    int dst = 0;
    for(std::size_t i = 0; i < subject_len; i++) {
        dst += (int) (uint8_t) ((char *) subject)[i];
    }

    for(std::size_t i = 0; i < predicate_len; i++) {
        dst += (int) (uint8_t) ((char *) predicate)[i];
    }

    return dst % num_datastores(hx);
}
