#ifndef HISTOGRAM_H
#define HISTOGRAM_H

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct histogram histogram_t;

typedef struct {
    double left;    // bucket values represent the left side of the range
    size_t count;   // count of how many values went into this range
} histogram_bucket_t;

extern const int HISTOGRAM_SUCCESS;
extern const int HISTOGRAM_ERROR;

/** @description Histogram::Histogram accessor functions only. C code never owns the histogram and should not allocate space for opaque pointers. */
int histogram_get_bucket_count(histogram_t *histogram, size_t *count);
int histogram_get_buckets(histogram_t *histogram, histogram_bucket_t *buckets);

#ifdef __cplusplus
}
#endif

#endif
