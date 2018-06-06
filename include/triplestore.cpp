#include "triplestore.hpp"

std::ostream &print_hex(std::ostream &stream, const void *data, const std::size_t len) {
    for(std::size_t i = 0; i < len; i++) {
        stream << std::setw(2) << std::setfill('0') << std::hex << (uint16_t) (uint8_t) ((const char *)data)[i] << " ";
    }
    return stream;
}

int convert2key(const void *first, std::size_t first_len,
                const void *second, std::size_t second_len,
                void **out, std::size_t *out_len) {
    if (!out    || !out_len    ||
        !first  || !first_len  ||
        !second || !second_len) {
        return HXHIM_ERROR;
    }

    *out_len = sizeof(first_len) + first_len + sizeof(second_len) + second_len;
    if (!(*out = ::operator new(*out_len))) {
        *out_len = 0;
        return HXHIM_ERROR;
    }

    char *curr = (char *) *out;

    // copy the first value
    memcpy(curr, first, first_len);
    curr += first_len;

    // copy the second value
    memcpy(curr, second, second_len);
    curr += second_len;

    // length of the first value
    encode_unsigned(curr, first_len);
    curr += sizeof(first_len);

    // length of the second value
    encode_unsigned(curr, second_len);

    return HXHIM_SUCCESS;
}
