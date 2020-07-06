#include <cstdlib>

#include "hxhim/accessors.hpp"
#include "hxhim/hash.hpp"

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
    int rank = -1;
    if (hxhim::GetMPI(hx, nullptr, &rank, nullptr) != HXHIM_SUCCESS) {
        return -1;
    }

    return rank;
}

/**
 * RankModDatastores
 * Returns the rank % datastores of the process as the target datastore
 *
 * @param hx            the HXHIM instance
 * @return the destination datastore ID or -1 on error
 */
int hxhim::hash::RankModDatastores(hxhim_t *hx, void *, const std::size_t, void *, const std::size_t, void *) {
    int rank = -1;
    std::size_t count = 0;
    if ((hxhim::GetMPI(hx, nullptr, &rank, nullptr) != HXHIM_SUCCESS) ||
        (hxhim::GetDatastoreCount(hx, &count)       != HXHIM_SUCCESS)) {
        return -1;
    }

    return rank % count;
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
    std::size_t count = 0;
    if (hxhim::GetDatastoreCount(hx, &count) != HXHIM_SUCCESS) {
        return -1;
    }

    int dst = 0;
    for(std::size_t i = 0; i < subject_len; i++) {
        dst += (int) (uint8_t) ((char *) subject)[i];
    }

    for(std::size_t i = 0; i < predicate_len; i++) {
        dst += (int) (uint8_t) ((char *) predicate)[i];
    }

    return dst % count;
}

/**
 * Left
 * Data from rank R goes to the first datastore on rank (R - 1) mod world_size
 *
 * @param hx            the HXHIM instance
 * @return the destination datastore ID or -1 on error
 */
int hxhim::hash::Left(hxhim_t *hx, void *, const std::size_t, void *, const std::size_t, void *) {
    int rank = -1;
    int size = -1;
    if (hxhim::GetMPI(hx, nullptr, &rank, &size) != HXHIM_SUCCESS) {
        return -1;
    }

    return (rank - 1) % size;
}

/**
 * Right
 * Data from rank R goes to the first datastore on rank (R + 1) mod world_size
 *
 * @param hx            the HXHIM instance
 * @return the destination datastore ID or -1 on error
 */
int hxhim::hash::Right(hxhim_t *hx, void *, const std::size_t, void *, const std::size_t, void *) {
    int rank = -1;
    int size = -1;
    if (hxhim::GetMPI(hx, nullptr, &rank, &size) != HXHIM_SUCCESS) {
        return -1;
    }

    return (rank + 1) % size;
}
