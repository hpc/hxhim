#ifndef HXHIM_HISTOGRAMS_H
#define HXHIM_HISTOGRAMS_H

#include <stddef.h>

#include "hxhim/constants.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_histograms_private hxhim_histograms_private_t;

/** @description This struct represents a collection of histograms */
typedef struct {
    hxhim_histograms_private_t *p;
} hxhim_histograms_t;

int hxhim_histograms_count(hxhim_histograms_t *hists, size_t *count);
int hxhim_histograms_get(hxhim_histograms_t *hists, const size_t idx,
                         double **buckets, size_t **counts, size_t *count);
int hxhim_histograms_destroy(hxhim_histograms_t *hists);

#ifdef __cplusplus
}
#endif

#endif
