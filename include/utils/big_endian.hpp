#ifndef HXHIM_BIG_ENDIAN_HPP
#define HXHIM_BIG_ENDIAN_HPP

#include <cstdint>
#include <cstring>

#include "hxhim/constants.h"

namespace big_endian {

template <typename T>
int run(void *dst, const void *src, std::size_t len) {
    if (!dst || !src) {
        return HXHIM_ERROR;
    }

    #if SYSTEM_BIG_ENDIAN
    std::memcpy(dst, src, len);
    #else
    if (sizeof(T) == 1) {
        std::memcpy(dst, src, len);
    }
    else {
        char *dst_ptr = (char *) dst;
        const char *src_ptr = (const char *) src;
        while (src_ptr && len) {
            dst_ptr[--len] = *src_ptr;
            src_ptr++;
        }
    }
    #endif

    return HXHIM_SUCCESS;
}

/** @description encoding of data into big endian */
template <typename T>
int encode(char *dst, const T &src, std::size_t len = sizeof(T)) {
    return run<T>(dst, &src, len);
}

template <typename T>
int encode(char *dst, T *src, std::size_t count) {
    return run<T>(dst, src, sizeof(T) * count);
}

/** @description decoding of big endian encoded data */
template <typename T>
int decode(T &dst, const char *src, std::size_t len = sizeof(T)) {
    return run<T>(&dst, src, len);
}

template <typename T>
int decode(T *dst, const char *src, std::size_t count) {
    return run<T>(dst, src, sizeof(T) * count);
}

}

#endif
