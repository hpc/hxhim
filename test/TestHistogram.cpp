#include "TestHistogram.hpp"

int CUSTOM_NONUNIFORM_FUNC(const double *, const size_t,
                           double **buckets, size_t *size,
                           void *) {
    if (!(*buckets = alloc_array<double>(3))) {
        return HISTOGRAM_ERROR;
    }

    (*buckets)[0] = 0;
    (*buckets)[1] = 5;
    (*buckets)[2] = 9;

    *size = 3;

    return HISTOGRAM_SUCCESS;
}
