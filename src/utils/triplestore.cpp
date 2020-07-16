#include <cstring>

#include "hxhim/constants.h"
#include "utils/big_endian.hpp"
#include "utils/memory.hpp"
#include "utils/triplestore.hpp"

/**
 * sp_to_key
 * Combines a subject and a predicate (if present) to form a key.
 *
 * @param fbp            the FixedBufferPool to allocate from
 * @param subject        the subject of the triple
 * @param predicate      the predicate of the triple
 * @param key            address of the key
 * @param key_len        address of the key length
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int sp_to_key(const Blob *subject,
              const Blob *predicate,
              void **key, std::size_t *key_len) {
    if (!subject || !predicate ||
        !key || !key_len) {
        return HXHIM_ERROR;
    }

    *key_len = subject->size() + sizeof(subject->size()) +
               predicate->size() + sizeof(predicate->size());

    if (!*key_len) {
        *key = nullptr;
        return HXHIM_SUCCESS;
    }

    *key = alloc(*key_len);

    char *curr = static_cast<char *>(*key);

    // copy the subject value
    memcpy(curr, subject->data(), subject->size());
    curr += subject->size();

    // length of the subject value
    encode_unsigned(curr, subject->size());
    curr += sizeof(subject->size());

    // copy the predicate value
    memcpy(curr, predicate->data(), predicate->size());
    curr += predicate->size();

    // length of the predicate value
    encode_unsigned(curr, predicate->size());

    return HXHIM_SUCCESS;
}

/**
 * key_to_sp
 * Splits a key into a subject key pair.
 *
 * @param key            the key
 * @param key_len        the key length
 * @param subject        the subject of the triple
 * @param predicate      the predicate of the triple
 * @paral copy           whether the subject and predicate are copies or references to the key
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int key_to_sp(const void *key, const std::size_t key_len,
              Blob **subject,
              Blob **predicate,
              const bool copy) {
    if (!key || !subject || !predicate) {
        return HXHIM_ERROR;
    }

    char *end = ((char *) key) + key_len;

    // read predicate
    std::size_t pred_len = 0;
    decode_unsigned(pred_len, end - sizeof(pred_len));
    void *pred_start = end - pred_len - sizeof(pred_len);

    if (copy) {
        void *pred = alloc(pred_len);
        memcpy(pred, pred_start, pred_len);
        *predicate = construct<RealBlob>(pred, pred_len);
    }
    else {
        *predicate = construct<ReferenceBlob>(pred_start, pred_len);
    }

    // read subject
    std::size_t sub_len = 0;
    decode_unsigned(sub_len, end - sizeof(sub_len) - pred_len - sizeof(pred_len));
    void *sub_start = (void *) key;

    if (copy) {
        void *sub = alloc(sub_len);
        memcpy(sub, sub_start, sub_len);
        *subject = construct<RealBlob>(sub, sub_len);
    }
    else {
        *subject = construct<ReferenceBlob>((void *) sub_start, sub_len);
    }

    return HXHIM_SUCCESS;
}
