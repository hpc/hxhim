#include "utils/elen.h"
#include "utils/elen.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>

const char ELEN_NEG = elen::NEG_SYMBOL;
const char ELEN_POS = elen::POS_SYMBOL;
const int  ELEN_ENCODE_FLOAT_PRECISION  = elen::encode::FLOAT_PRECISION;
const int  ELEN_ENCODE_DOUBLE_PRECISION = elen::encode::DOUBLE_PRECISION;

template <>
int elen::encode::default_precision<float>() {
    return FLOAT_PRECISION;
}

template <>
int elen::encode::default_precision<double>() {
    return DOUBLE_PRECISION;
}

/**
 * lex_comp::operator()
 * Helper function for std::lexicographical_compare.
 *
 * @param lhs the string on the left hand side
 * @param rhs the string on the right hand side
 * @return whether or not lhs comes before rhs
 */
bool lex_comp(const std::string &lhs, const std::string &rhs) {
    return std::lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
}

static char * c_str(const std::string & str) {
    char * ret = (char *) malloc((str.size() + 1) * sizeof(char));
    std::memcpy(ret, str.c_str(), str.size());
    ret[str.size()] = '\0';
    return ret;
}

#define elen_encode_int(type)                                                     \
    char * elen_encode_##type(const type value, const char neg, const char pos) { \
        return c_str(elen::encode::integers <type> (value, neg, pos));            \
    }                                                                             \

elen_encode_int(int);
elen_encode_int(uint8_t);
elen_encode_int(uint16_t);
elen_encode_int(uint32_t);
elen_encode_int(uint64_t);
elen_encode_int(int8_t);
elen_encode_int(int16_t);
elen_encode_int(int32_t);
elen_encode_int(int64_t);

#define elen_encode_small_decimals(type)                                                                     \
    char * elen_encode_small_##type(const type value, const int precision, const char neg, const char pos) { \
        return c_str(elen::encode::small_decimals(value, precision, neg, pos));                              \
    }                                                                                                        \

elen_encode_small_decimals(float);
elen_encode_small_decimals(double);

#define elen_encode_large_decimals(type)                                                                     \
    char * elen_encode_large_##type(const type value, const int precision, const char neg, const char pos) { \
        return c_str(elen::encode::large_decimals(value, precision, neg, pos));                              \
    }                                                                                                        \

elen_encode_large_decimals(float);
elen_encode_large_decimals(double);

#define elen_encode_floating_point(type)                                                                        \
    char * elen_encode_##type(const type value, const int precision, const char neg, const char pos) { \
        return c_str(elen::encode::floating_point(value, precision, neg, pos));                                 \
    }                                                                                                           \

elen_encode_floating_point(float);
elen_encode_floating_point(double);

#define elen_decode_int(type)                                                                        \
    type elen_decode_##type(const char * str, size_t * prefix_len, const char neg, const char pos) { \
        return elen::decode::integers<type> (str, prefix_len, neg, pos);                             \
    }                                                                                                \

elen_decode_int(int);
elen_decode_int(uint8_t);
elen_decode_int(uint16_t);
elen_decode_int(uint32_t);
elen_decode_int(uint64_t);
elen_decode_int(int8_t);
elen_decode_int(int16_t);
elen_decode_int(int32_t);
elen_decode_int(int64_t);

#define elen_decode_small_decimals(type)                                                \
    type elen_decode_small_##type(const char * str, const char neg, const char pos) {   \
        return elen::decode::small_decimals <type> (str, neg, pos);                     \
    }                                                                                   \

elen_decode_small_decimals(float);
elen_decode_small_decimals(double);

#define elen_decode_large_decimals(type)                                                \
    type elen_decode_large_##type(const char * str, const char neg, const char pos) {   \
        return elen::decode::large_decimals <type> (str, neg, pos);                     \
    }                                                                                   \

elen_decode_large_decimals(float);
elen_decode_large_decimals(double);

#define elen_decode_floating_point(type)                                                \
    type elen_decode_##type(const char * str, const char neg, const char pos) {         \
        return elen::decode::floating_point <type> (str, neg, pos);                     \
    }                                                                                   \

elen_decode_floating_point(float);
elen_decode_floating_point(double);

int lex_comp(const void * lhs, const void * rhs) {
    const char * l = (const char *) lhs;
    const char * r = (const char *) rhs;

    /* not checking for NULL inputs */
    while (*l && *r) {
        if (*l < *r) {
            return 1;
        }
        else if (*l > * r) {
            return 0;
        }

        l++;
        r++;
    }

    return (*l < *r);
}
