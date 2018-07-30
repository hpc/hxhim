#include "hxhim/triplestore.hpp"

/**
 * sp_to_key
 * Combines a subject and a predicate (if present) to form a key.
 *
 * @param subject        the subject of the triple
 * @param subject_len    the length of the subject
 * @param predicate      the predicate of the triple
 * @param predicate_len  the length of the predicate
 * @param key            address of the key
 * @param key_len        address of the key length
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int sp_to_key(const void *subject, const std::size_t subject_len,
              const void *predicate, const std::size_t predicate_len,
              void **key, std::size_t *key_len) {
    if (!key       || !key_len     ||
        !subject   || !subject_len) {
        return HXHIM_ERROR;
    }

    *key_len = subject_len + sizeof(subject_len);
    if (predicate) {
        *key_len += predicate_len + sizeof(predicate_len);
    }

    if (!*key_len) {
        *key = nullptr;
        return HXHIM_SUCCESS;
    }

    if (!(*key = ::operator new (*key_len))) {
        *key_len = 0;
        return HXHIM_ERROR;
    }

    char *curr = (char *) *key;

    if (subject && subject_len) {
        // copy the subject value
        memcpy(curr, subject, subject_len);
        curr += subject_len;

        // length of the subject value
        encode_unsigned(curr, subject_len);
        curr += sizeof(subject_len);
    }

    if (predicate && predicate_len) {
        // copy the predicate value
        memcpy(curr, predicate, predicate_len);
        curr += predicate_len;

        // length of the predicate value
        encode_unsigned(curr, predicate_len);
    }

    return HXHIM_SUCCESS;
}

/**
 * key_to_sp
 * Splits a key into a subject key pair. No memory is allocated.
 * The key must be valid until the outputs of this function are
 * no longer needed.
 *
 * @param key            the key
 * @param key_len        the key length
 * @param subject        address of the subject of the triple
 * @param subject_len    address of the length of the subject
 * @param predicate      address of the predicate of the triple
 * @param predicate_len  address of the length of the predicate
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int key_to_sp(const void *key, const std::size_t key_len,
              void **subject, std::size_t *subject_len,
              void **predicate, std::size_t *predicate_len) {
    if (!key) {
        return HXHIM_ERROR;
    }

    // read predicate
    std::size_t pred_len = 0;
    decode_unsigned((char *) key + key_len - sizeof(*predicate_len), pred_len);

    if (predicate) {
        *predicate = (char *) key + key_len - pred_len - sizeof(*predicate_len);
    }

    if (predicate_len) {
        *predicate_len = pred_len;
    }

    // read subject
    std::size_t sub_len = 0;
    decode_unsigned((char *) key + key_len - sizeof(*subject_len) - pred_len - sizeof(*predicate_len), sub_len);

    if (subject) {
        *subject = (char *) key;
    }

    if (subject_len) {
        *subject_len = sub_len;
    }

    return HXHIM_SUCCESS;
}
