#ifndef ELEN_H
#define ELEN_H

#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * C wrappers for elen.hpp
 *
 * The encoded buffers should be freed by the user.
 * The encoded buffers are NULL terminated.
 */

int elen_encode_int(const int value, char **encoded, size_t *encoded_len);
int elen_decode_int(const char *encoded, const size_t encoded_size, int *decoded);

int elen_encode_size_t(const size_t value, char **encoded, size_t *encoded_len);
int elen_decode_size_t(const char *encoded, const size_t encoded_size, size_t *decoded);

int elen_encode_float(const float value, const int precision, char **encoded, size_t *encoded_len);
int elen_decode_float(const char *encoded, const size_t encoded_size, float *decoded);

int elen_encode_double(const double value, const int precision, char **encoded, size_t *encoded_len);
int elen_decode_double(const char *encoded, const size_t encoded_size, double *decoded);

#ifdef __cplusplus
}
#endif

#endif
