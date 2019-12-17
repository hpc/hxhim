#ifndef HXHIM_TRIPLESTORE
#define HXHIM_TRIPLESTORE

#include <cstring>
#include <type_traits>

#include "hxhim/constants.h"
#include "utils/type_traits.hpp"
#include "utils/memory.hpp"

/** @description encoding of unsigned integral types into big endian */
template <typename Z, typename = enable_if_t<std::is_unsigned<Z>::value> >
int encode_unsigned(void *buf, Z val, std::size_t len = sizeof(Z)) {
    if (!buf) {
        return HXHIM_ERROR;
    }

    memset(buf, 0, len);
    while (val && len) {
        ((char *) buf)[--len] = val & 0xffU;
        val >>= 8;
    }

    return HXHIM_SUCCESS;
}

/** @description decoding of big endian encoded unsigned integral types */
template <typename Z, typename = enable_if_t<std::is_unsigned<Z>::value> >
int decode_unsigned(void *buf, Z &val, std::size_t len = sizeof(Z)) {
    if (!buf) {
        return HXHIM_ERROR;
    }

    val = 0;
    for(std::size_t i = 0; i < len; i++) {
        val = (val << 8) | ((char *) buf)[i];
    }

    return HXHIM_SUCCESS;
}

/** @description Combines a subject and predicate into a key */
int sp_to_key(const void *subject, const std::size_t subject_len,
              const void *predicate, const std::size_t predicate_len,
              void **key, std::size_t *key_len);

/** @description Splits a key into a subject and predicate */
int key_to_sp(const void *key, const std::size_t key_len,
              void **subject, std::size_t *subject_len,
              void **predicate, std::size_t *predicate_len);
#endif
