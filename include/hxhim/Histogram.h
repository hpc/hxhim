#ifndef HXHIM_HISTOGRAM_H
#define HXHIM_HISTOGRAM_H

#include <stddef.h>

#include "hxhim/constants.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_histogram_private hxhim_histogram_private_t;

typedef struct {
    hxhim_histogram_private_t *p;
} hxhim_histogram_t;

int hxhim_histogram_count(hxhim_histogram_t *hists, size_t *count);
int hxhim_histogram_get(hxhim_histogram_t *hists, const size_t idx,
                        double **buckets, size_t **counts, size_t *count);

#ifdef __cplusplus
}
#endif

#endif
