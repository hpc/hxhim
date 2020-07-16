#ifndef HXHIM_PACK_HPP
#define HXHIM_PACK_HPP

#include <cstdint>
#include <cstring>

#include "hxhim/constants.h"
#include "utils/type_traits.hpp"

/** @description encoding of unsigned integral types into big endian */
template <typename Z, typename = enable_if_t<std::is_unsigned<Z>::value> >
int encode_unsigned(void *dst, Z src, std::size_t len = sizeof(Z)) {
    if (!dst) {
        return HXHIM_ERROR;
    }

    memset(dst, 0, len);
    while (src && len) {
        ((char *) dst)[--len] = (uint8_t) (src & 0xffU);
        src >>= 8;
    }

    return HXHIM_SUCCESS;
}

/** @description decoding of big endian encoded unsigned integral types */
template <typename Z, typename = enable_if_t<std::is_unsigned<Z>::value> >
int decode_unsigned(Z &dst, void *src, const std::size_t len = sizeof(Z)) {
    if (!src) {
        return HXHIM_ERROR;
    }

    dst = 0;
    for(std::size_t i = 0; i < len; i++) {
        dst = (dst << 8) | ((uint8_t) ((char *) src)[i]);
    }

    return HXHIM_SUCCESS;
}

#endif
