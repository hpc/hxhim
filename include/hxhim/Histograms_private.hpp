#ifndef HXHIM_HISTOGRAMS_PRIVATE_H
#define HXHIM_HISTOGRAMS_PRIVATE_H

#include <vector>

#include "hxhim/Histograms.hpp"
#include "hxhim/struct.h"
#include "utils/Histogram.hpp"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_histograms_private {
    std::vector<Histogram::Histogram *> hists;
} hxhim_histograms_private_t;

#ifdef __cplusplus
}
#endif

namespace hxhim {
namespace Histograms {
int init(hxhim_t *hx, hxhim_histograms_t **hists);
}
}

#endif
