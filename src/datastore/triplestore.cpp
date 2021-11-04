#include <cstring>

#include "hxhim/constants.h"
#include "datastore/triplestore.hpp"
#include "utils/little_endian.hpp"
#include "utils/memory.hpp"

/**
 * sp_to_key
 * Combines a subject and a predicate to form a key.
 * The buffer to store the key should be allocated
 * by the caller.
 *
 * subject + predicate + '\xff' + subject len + predicate_len + subject type + predicate type
 *
 * The '\xff' terminates the key when one subject + predicate
 * is a prefix of another subject + predicate
 *
 *
 * @param subject        the subject of the triple
 * @param predicate      the predicate of the triple
 * @param key            the formatted key - will be filled in
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int sp_to_key(const Blob &subject,
              const Blob &predicate,
              Blob *key) {
    // add 1 to both for types
    const std::size_t len = subject.pack_size(false) +
                            predicate.pack_size(false) +
                            3 * sizeof(uint8_t);

    char *buf = (char *) alloc(len);
    char *curr = buf;

    // copy the subject value
    memcpy(curr, subject.data(), subject.size());
    curr += subject.size();

    // copy the predicate value
    memcpy(curr, predicate.data(), predicate.size());
    curr += predicate.size();

    // terminate the key
    *curr = '\x0ff';
    curr++;

    // length of the subject value
    little_endian::encode(curr, subject.size());
    curr += sizeof(subject.size());

    // length of the predicate value
    little_endian::encode(curr, predicate.size());
    curr += sizeof(predicate.size());

    // the subject type
    *curr = subject.data_type();
    curr += sizeof(uint8_t);

    // the predicate type
    *curr = predicate.data_type();
    curr += sizeof(uint8_t);

    *key = std::move(RealBlob(buf, len, hxhim_data_t::HXHIM_DATA_BYTE));

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
int key_to_sp(const Blob &key,
              Blob &subject,
              Blob &predicate,
              const bool copy) {
    char *end = ((char *) key.data()) + key.size();

    // read predicate type
    uint8_t pred_type = HXHIM_DATA_INVALID;
    const char *pred_type_start = end - sizeof(pred_type);
    pred_type = *pred_type_start;

    // read subject type
    uint8_t sub_type = HXHIM_DATA_INVALID;
    const char *sub_type_start = pred_type_start - sizeof(sub_type);
    sub_type = *sub_type_start;

    // read predicate len
    std::size_t pred_len = 0;
    const char *pred_len_start = sub_type_start - sizeof(pred_len);
    little_endian::decode(pred_len, pred_len_start);

    // read subject len
    std::size_t sub_len = 0;
    const char *sub_len_start = pred_len_start - sizeof(sub_len);
    little_endian::decode(sub_len, sub_len_start);

    // read terminator character
    const char *check = sub_len_start - 1;

    // read predicate
    const char *pred_start = check - pred_len;
    if (copy) {
        predicate = Blob(pred_len, pred_start, (hxhim_data_t) pred_type);
    }
    else {
        predicate = ReferenceBlob((void *) pred_start, pred_len, (hxhim_data_t) pred_type);
    }

    // read subject
    const char *sub_start = (const char *) key.data();

    if (copy) {
        subject = Blob(sub_len, sub_start, (hxhim_data_t) sub_type);
    }
    else {
        subject = ReferenceBlob((void *) sub_start, sub_len, (hxhim_data_t) sub_type);
    }

    return HXHIM_SUCCESS;
}
