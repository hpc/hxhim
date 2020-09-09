#include <cstring>

#include "hxhim/constants.h"
#include "utils/big_endian.hpp"
#include "utils/memory.hpp"
#include "utils/triplestore.hpp"

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
 * @return the original buf pointer, or nullptr on error
 */
char *sp_to_key(const Blob &subject,
                const Blob &predicate,
                char *&buf, std::size_t &buf_len,
                std::size_t &key_len) {
    if (!subject.data()   ||
        !predicate.data() ||
        !buf || !buf_len)  {
        return nullptr;
    }

    // Blob packs length + value
    // this function packs value + length, but sizes are the same
    key_len = subject.pack_size() + predicate.pack_size();

    // check the buffer length
    if (buf_len < key_len) {
        return nullptr;
    }

    char *orig = buf;

    // copy the subject value
    memcpy(buf, subject.data(), subject.size());
    buf += subject.size();

    // length of the subject value
    big_endian::encode(buf, subject.size());
    buf += sizeof(subject.size());

    // copy the predicate value
    memcpy(buf, predicate.data(), predicate.size());
    buf += predicate.size();

    // length of the predicate value
    big_endian::encode(buf, predicate.size());
    buf += sizeof(predicate.size());

    buf_len -= key_len;

    return orig;
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
              Blob &subject,
              Blob &predicate,
              const bool copy) {
    if (!key) {
        return HXHIM_ERROR;
    }

    char *end = ((char *) key) + key_len;

    // read predicate
    std::size_t pred_len = 0;
    big_endian::decode(pred_len, end - sizeof(pred_len));
    void *pred_start = end - pred_len - sizeof(pred_len);

    if (copy) {
        void *pred = alloc(pred_len);
        memcpy(pred, pred_start, pred_len);
        predicate = RealBlob(pred, pred_len);
    }
    else {
        predicate = ReferenceBlob(pred_start, pred_len);
    }

    // read subject
    std::size_t sub_len = 0;
    big_endian::decode(sub_len, end - sizeof(sub_len) - pred_len - sizeof(pred_len));
    void *sub_start = (void *) key;

    if (copy) {
        void *sub = alloc(sub_len);
        memcpy(sub, sub_start, sub_len);
        subject = RealBlob(sub, sub_len);
    }
    else {
        subject = ReferenceBlob((void *) sub_start, sub_len);
    }

    return HXHIM_SUCCESS;
}
