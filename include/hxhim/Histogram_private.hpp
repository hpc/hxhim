#ifndef HXHIM_HISTOGRAM_PRIVATE_H
#define HXHIM_HISTOGRAM_PRIVATE_H

#include <vector>

#include "hxhim/Histogram.hpp"
#include "hxhim/struct.h"
#include "utils/Histogram.hpp"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_histogram_private {
    std::vector<Histogram::Histogram *> hists;
} hxhim_histogram_private_t;

#ifdef __cplusplus
}
#endif

namespace hxhim {
namespace histogram {
int init(hxhim_t *hx, hxhim_histogram_t **hists);
int destroy(hxhim_histogram_t *hists);
}
}

#endif
