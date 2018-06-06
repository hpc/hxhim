#include <cstring>
#include <string>

#include "elen.h"
#include "elen.hpp"

int elen_encode_int(const int value, char **encoded, size_t *encoded_len) {
    if (!encoded) {
        return MDHIM_ERROR;
    }

    const std::string str = elen::encode::integers(value);
    *encoded = (char *) calloc(str.size() + 1, sizeof(char));
    if (!*encoded) {
        return MDHIM_ERROR;
    }
    memcpy(*encoded, str.c_str(), str.size());
    if (encoded_len) {
        *encoded_len = str.size();
    }
    return MDHIM_SUCCESS;
}

int elen_decode_int(const char *encoded, const size_t encoded_size, int *decoded) {
    if (!decoded) {
        return MDHIM_ERROR;
    }

    *decoded = elen::decode::integers<int>(std::string(encoded, encoded_size));

    return MDHIM_SUCCESS;
}

int elen_encode_size_t(const size_t value, char **encoded, size_t *encoded_len) {
    if (!encoded) {
        return MDHIM_ERROR;
    }

    const std::string str = elen::encode::integers(value);
    *encoded = (char *) calloc(str.size() + 1, sizeof(char));
    if (!*encoded) {
        return MDHIM_ERROR;
    }
    memcpy(*encoded, str.c_str(), str.size());
    if (encoded_len) {
        *encoded_len = str.size();
    }
    return MDHIM_SUCCESS;
}

int elen_decode_size_t(const char *encoded, const size_t encoded_size, size_t *decoded) {
    if (!decoded) {
        return MDHIM_ERROR;
    }

    *decoded = elen::decode::integers<std::size_t>(std::string(encoded, encoded_size));

    return MDHIM_SUCCESS;
}

int elen_encode_float(const float value, const int precision, char **encoded, size_t *encoded_len) {
    if (!encoded) {
        return MDHIM_ERROR;
    }

    const std::string str = elen::encode::floating_point(value, precision);
    *encoded = (char *) calloc(str.size() + 1, sizeof(char));
    if (!*encoded) {
        return MDHIM_ERROR;
    }
    memcpy(*encoded, str.c_str(), str.size());
    if (encoded_len) {
        *encoded_len = str.size();
    }
    return MDHIM_SUCCESS;
}

int elen_decode_float(const char *encoded, const size_t encoded_size, float *decoded) {
    if (!decoded) {
        return MDHIM_ERROR;
    }

    *decoded = elen::decode::floating_point<float>(std::string(encoded, encoded_size));

    return MDHIM_SUCCESS;
}

int elen_encode_double(const double value, const int precision, char **encoded, size_t *encoded_len) {
    if (!encoded) {
        return MDHIM_ERROR;
    }

    const std::string str = elen::encode::floating_point(value, precision);
    *encoded = (char *) calloc(str.size() + 1, sizeof(char));
    if (!*encoded) {
        return MDHIM_ERROR;
    }
    memcpy(*encoded, str.c_str(), str.size());
    if (encoded_len) {
        *encoded_len = str.size();
    }
    return MDHIM_SUCCESS;
}

int elen_decode_double(const char *encoded, const size_t encoded_size, double *decoded) {
    if (!decoded) {
        return MDHIM_ERROR;
    }

    *decoded = elen::decode::floating_point<double>(std::string(encoded, encoded_size));

    return MDHIM_SUCCESS;
}
