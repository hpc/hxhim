#include "hxhim/hash.h"
#include "hxhim/private/accessors.hpp"
#include "utils/uthash.h"

/**
 * hxhim_hash
 * This function wraps the hash function set by the
 * configuration and performs generic checks.
 *
 * @param hx            the HXHIM instance
 * @param subject       the subject to hash
 * @param subject_len   the length of the subject
 * @param predicate     the predicate to hash
 * @param predicate_len the length of the predicate
 * @return the destination datastore ID or -1 on error
 */
int hxhim_hash(hxhim_t *hx,
               void *subject, const size_t subject_len,
               void *predicate, const size_t predicate_len) {
    hxhim_hash_t func = nullptr;
    void *args = nullptr;
    if (hxhim::nocheck::GetHash(hx, nullptr, &func, &args) != HXHIM_SUCCESS) {
        return -1;
    }

    return func(hx,
                subject, subject_len,
                predicate, predicate_len,
                args);
}

/**
 * hxhim_hash_DataStoreZero
 *
 * @return 0
 */
int hxhim_hash_DatastoreZero(hxhim_t *, void *, const size_t, void *, const size_t, void *) {
    return 0;
}

/**
 * hxhim_hash_RankModDatastores
 * Returns the rank % datastores of the process as the target datastore
 *
 * @param hx            the HXHIM instance
 * @return the destination datastore ID or -1 on error
 */
int hxhim_hash_RankModDatastores(hxhim_t *hx, void *, const size_t, void *, const size_t, void *) {
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    std::size_t count = 0;
    hxhim::nocheck::GetDatastoreCount(hx, &count);

    return rank % count;
}

/**
 * hxhim_hash_SumModDatastores
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
int hxhim_hash_SumModDatastores(hxhim_t *hx, void *subject, const size_t subject_len, void *predicate, const size_t predicate_len, void *) {
    std::size_t count = 0;
    hxhim::nocheck::GetDatastoreCount(hx, &count);

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
 * hxhim_hash_uthash_BER
 *
 * @param hx            the HXHIM instance
 * @param subject       the subject to hash
 * @param subject_len   the length of the subject
 * @param predicate     the predicate to hash
 * @param predicate_len the length of the predicate
 * @return the destination datastore ID or -1 on error
 */
int hxhim_hash_uthash_BER(hxhim_t *hx, void *subject, const size_t subject_len, void *predicate, const size_t predicate_len, void *) {
    std::size_t count = 0;
    hxhim::nocheck::GetDatastoreCount(hx, &count);

    unsigned hashv = 0;
    HASH_BER(subject,   subject_len,   hashv);
    HASH_BER(predicate, predicate_len, hashv);

    return hashv % count;
}

/**
 * hxhim_hash_uthash_SAX
 *
 * @param hx            the HXHIM instance
 * @param subject       the subject to hash
 * @param subject_len   the length of the subject
 * @param predicate     the predicate to hash
 * @param predicate_len the length of the predicate
 * @return the destination datastore ID or -1 on error
 */
int hxhim_hash_uthash_SAX(hxhim_t *hx, void *subject, const size_t subject_len, void *predicate, const size_t predicate_len, void *) {
    std::size_t count = 0;
    hxhim::nocheck::GetDatastoreCount(hx, &count);

    unsigned hashv = 0;
    HASH_SAX(subject,   subject_len,   hashv);
    HASH_SAX(predicate, predicate_len, hashv);
    return hashv % count;
}

/**
 * hxhim_hash_uthash_FNV
 *
 * @param hx            the HXHIM instance
 * @param subject       the subject to hash
 * @param subject_len   the length of the subject
 * @param predicate     the predicate to hash
 * @param predicate_len the length of the predicate
 * @return the destination datastore ID or -1 on error
 */
int hxhim_hash_uthash_FNV(hxhim_t *hx, void *subject, const size_t subject_len, void *predicate, const size_t predicate_len, void *) {
    std::size_t count = 0;
    hxhim::nocheck::GetDatastoreCount(hx, &count);

    unsigned hashv = 2166136261U;
    HASH_FNV(subject,   subject_len,   hashv);
    HASH_FNV(predicate, predicate_len, hashv);
    return hashv % count;
}

/**
 * hxhim_hash_uthash_OAT
 *
 * @param hx            the HXHIM instance
 * @param subject       the subject to hash
 * @param subject_len   the length of the subject
 * @param predicate     the predicate to hash
 * @param predicate_len the length of the predicate
 * @return the destination datastore ID or -1 on error
 */
int hxhim_hash_uthash_OAT(hxhim_t *hx, void *subject, const size_t subject_len, void *predicate, const size_t predicate_len, void *) {
    std::size_t count = 0;
    hxhim::nocheck::GetDatastoreCount(hx, &count);

    unsigned hashv = 0;
    HASH_OAT(subject,   subject_len,   hashv);
    HASH_OAT(predicate, predicate_len, hashv);
    return hashv % count;
}

/**
 * hxhim_hash_uthash_JEN
 *
 * @param hx            the HXHIM instance
 * @param subject       the subject to hash
 * @param subject_len   the length of the subject
 * @param predicate     the predicate to hash
 * @param predicate_len the length of the predicate
 * @return the destination datastore ID or -1 on error
 */
int hxhim_hash_uthash_JEN(hxhim_t *hx, void *subject, const size_t subject_len, void *predicate, const size_t predicate_len, void *) {
    std::size_t count = 0;
    hxhim::nocheck::GetDatastoreCount(hx, &count);

    unsigned hashv = 0xfeedbeefu;
    HASH_JEN(subject,   subject_len,   hashv);
    HASH_JEN(predicate, predicate_len, hashv);
    return hashv % count;
}

/**
 * hxhim_hash_uthash_SFH
 *
 * @param hx            the HXHIM instance
 * @param subject       the subject to hash
 * @param subject_len   the length of the subject
 * @param predicate     the predicate to hash
 * @param predicate_len the length of the predicate
 * @return the destination datastore ID or -1 on error
 */
int hxhim_hash_uthash_SFH(hxhim_t *hx, void *subject, const size_t subject_len, void *predicate, const size_t predicate_len, void *) {
    std::size_t count = 0;
    hxhim::nocheck::GetDatastoreCount(hx, &count);

    unsigned hashv = 0xcafebabeu;
    HASH_SFH(subject,   subject_len,   hashv);
    HASH_SFH(predicate, predicate_len, hashv);
    return hashv % count;
}
