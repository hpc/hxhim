#include <cstring>

#include "hxhim/constants.h"
#include "hxhim/triplestore.hpp"
#include "utils/little_endian.hpp"
#include "utils/memory.hpp"

/**
 * sp_to_key
 * Combines a subject and a predicate to form a key.
 * The buffer to store the key should be allocated
 * by the caller.
 *
 * @param subject        the subject of the triple
 * @param predicate      the predicate of the triple
 * @param buf            start of key buffer (updated to next unused address)
 * @param buf_len        available space for data (updated to remaining space)
 * @param key_len        the length of the key
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int sp_to_key(const Blob &subject,
              const Blob &predicate,
              std::string &key) {
    // Blob packs length + value
    // this function packs value + length, but sizes are the same
    const std::size_t len = subject.pack_size(false) + predicate.pack_size(false);

    char *buf = (char *) alloc(len);
    char *curr = buf;

    // copy the subject value
    memcpy(curr, subject.data(), subject.size());
    curr += subject.size();

    // length of the subject value
    little_endian::encode(curr, subject.size());
    curr += sizeof(subject.size());

    // copy the predicate value
    memcpy(curr, predicate.data(), predicate.size());
    curr += predicate.size();

    // length of the predicate value
    little_endian::encode(curr, predicate.size());
    curr += sizeof(predicate.size());

    key.assign(buf, len);

    dealloc(buf);

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
int key_to_sp(const void *key,
              const std::size_t key_len,
              Blob &subject,
              Blob &predicate,
              const bool copy) {
    char *end = ((char *) key) + key_len;

    // read predicate
    std::size_t pred_len = 0;
    little_endian::decode(pred_len, end - sizeof(pred_len));
    const void *pred_start = end - pred_len - sizeof(pred_len);

    if (copy) {
        void *pred = alloc(pred_len);
        memcpy(pred, pred_start, pred_len);
        predicate = RealBlob(pred, pred_len, hxhim_data_t::HXHIM_DATA_BYTE);
    }
    else {
        predicate = ReferenceBlob((void *) pred_start, pred_len, hxhim_data_t::HXHIM_DATA_BYTE);
    }

    // read subject
    std::size_t sub_len = 0;
    little_endian::decode(sub_len, end - sizeof(sub_len) - pred_len - sizeof(pred_len));
    void *sub_start = (void *) key;

    if (copy) {
        void *sub = alloc(sub_len);
        memcpy(sub, sub_start, sub_len);
        subject = RealBlob(sub, sub_len, hxhim_data_t::HXHIM_DATA_BYTE);
    }
    else {
        subject = ReferenceBlob((void *) sub_start, sub_len, hxhim_data_t::HXHIM_DATA_BYTE);
    }

    return HXHIM_SUCCESS;
}

/**
 * key_to_sp
 * Splits a key into a subject key pair.
 *
 * @param key            the key
 * @param subject        the subject of the triple
 * @param predicate      the predicate of the triple
 * @paral copy           whether the subject and predicate are copies or references to the key
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
int key_to_sp(const std::string &key,
              Blob &subject,
              Blob &predicate,
              const bool copy) {
    return key_to_sp((void *) key.data(), key.size(),
                     subject, predicate,
                     copy);
}
