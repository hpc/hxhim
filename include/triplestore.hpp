#ifndef HXHIM_TRIPLESTORE
#define HXHIM_TRIPLESTORE

#include <cstring>
#include <iomanip>
#include <sstream>
#include <type_traits>

#include "elen.hpp"
#include "hxhim-types.h"

template <typename Z, typename = std::enable_if_t<std::is_unsigned<Z>::value> >
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

template <typename Z, typename = std::enable_if_t<std::is_unsigned<Z>::value> >
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

std::ostream &print_hex(std::ostream &stream, const void *data, const std::size_t len);

int convert2key(const void *first, std::size_t first_len,
                const void *second, std::size_t second_len,
                void **out, std::size_t *out_len);
#endif
