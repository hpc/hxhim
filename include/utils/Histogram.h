#ifndef HISTOGRAM_H
#define HISTOGRAM_H

#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

#define HISTOGRAM_SUCCESS  0
#define HISTOGRAM_ERROR   -1

typedef struct histogram histogram_t;

/**
 * HistogramBucketGenerator_t
 * Typedef of function type which takes in
 *    - an array of initial values
 *    - the histogram buckets to allocate and set
 *    - extra arguments the function needs
 *
 * @param first_n     first n data points
 * @param n           the number of data points
 * @param buckets     the address of the bucket array
 * @param size        how many buckets there are
 * @param extra       extra arguments for the function
 * @return HISTOGRAM_SUCCESS or HISTOGRAM_ERROR on error
 */
typedef int (*HistogramBucketGenerator_t)(const double *first_n, const size_t n,
                                          double **buckets, size_t *size,
                                          void *extra);

/**
 * Predefined bucket generation functions
 * Some were taken from https://en.wikipedia.org/wiki/Histogram#Number_of_bins_and_width
 */
int histogram_n_buckets                   (const double *first_n, const size_t n, double **buckets, size_t *size, void *extra);
int histogram_square_root_choice          (const double *first_n, const size_t n, double **buckets, size_t *size, void *extra);
int histogram_sturges_formula             (const double *first_n, const size_t n, double **buckets, size_t *size, void *extra);
int histogram_rice_rule                   (const double *first_n, const size_t n, double **buckets, size_t *size, void *extra);
int histogram_scotts_normal_reference_rule(const double *first_n, const size_t n, double **buckets, size_t *size, void *extra);
int histogram_uniform_logn                (const double *first_n, const size_t n, double **buckets, size_t *size, void *extra);

#ifdef __cplusplus
}
#endif

#endif
