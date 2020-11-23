#ifndef EFFICIENT_LEXICOGRAPHIC_ENCODING_OF_NUMBERS_H
#define EFFICIENT_LEXICOGRAPHIC_ENCODING_OF_NUMBERS_H

#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

extern const char ELEN_NEG;
extern const char ELEN_POS;
extern const int ELEN_ENCODE_FLOAT_PRECISION;
extern const int ELEN_ENCODE_DOUBLE_PRECISION;

/*  Chapter 3 Integers */
#define elen_encode_int_prototype(type)                                 \
    char * elen_encode_##type(const type value, const char neg, const char pos)

elen_encode_int_prototype(int);
elen_encode_int_prototype(uint8_t);
elen_encode_int_prototype(uint16_t);
elen_encode_int_prototype(uint32_t);
elen_encode_int_prototype(uint64_t);
elen_encode_int_prototype(int8_t);
elen_encode_int_prototype(int16_t);
elen_encode_int_prototype(int32_t);
elen_encode_int_prototype(int64_t);

/*  Chapter 4 Small Decimals */
#define elen_encode_small_decimals_prototype(type)                      \
    char * elen_encode_small_##type(const type value, const int precision, const char neg, const char pos)

elen_encode_small_decimals_prototype(float);
elen_encode_small_decimals_prototype(double);

/*  Chapter 5 Large Decimals */
#define elen_encode_large_decimals_prototype(type)                      \
    char * elen_encode_large_##type(const type value, const int precision, const char neg, const char pos)

elen_encode_large_decimals_prototype(float);
elen_encode_large_decimals_prototype(double);

/*  Chapter 6 Floating Pointer Numbers */
#define elen_encode_floating_point_prototype(type)                      \
    char * elen_encode_floating_##type(const type value, const int precision, const char neg, const char pos)

elen_encode_floating_point_prototype(float);
elen_encode_floating_point_prototype(double);

/*  Chapter 3 Integers */
#define elen_decode_int_prototype(type)                                 \
    type elen_decode_##type(const char * str, size_t * prefix_len, const char neg, const char pos)

elen_decode_int_prototype(int);
elen_decode_int_prototype(uint8_t);
elen_decode_int_prototype(uint16_t);
elen_decode_int_prototype(uint32_t);
elen_decode_int_prototype(uint64_t);
elen_decode_int_prototype(int8_t);
elen_decode_int_prototype(int16_t);
elen_decode_int_prototype(int32_t);
elen_decode_int_prototype(int64_t);

/*  Chapter 4 Small Decimals */
#define elen_decode_small_decimals_prototype(type)                      \
    type elen_decode_small_##type(const char * str, const char neg, const char pos)

elen_decode_small_decimals_prototype(float);
elen_decode_small_decimals_prototype(double);

/*  Chapter 5 Large Decimals */
#define elen_decode_large_decimals_prototype(type)                      \
    type elen_decode_large_##type(const char * str, const char neg, const char pos)

elen_decode_large_decimals_prototype(float);
elen_decode_large_decimals_prototype(double);

/*  Chapter 6 Floating Pointer Numbers */
#define elen_decode_floating_point_prototype(type)                      \
    type elen_decode_floating_##type(const char * str, const char neg, const char pos)

elen_decode_floating_point_prototype(float);
elen_decode_floating_point_prototype(double);

/* @description Use this function for lexicographic comparisions */
/* returns lhs < rhs */
int lex_comp(const void * lhs, const void * rhs);

#ifdef __cplusplus
}
#endif

#endif
