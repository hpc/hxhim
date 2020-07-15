#ifndef HXHIM_HISTOGRAM_HPP
#define HXHIM_HISTOGRAM_HPP

#include <cstddef>

#include "hxhim/Histograms.h"

namespace hxhim {
namespace Histograms {

int Count(hxhim_histograms_t *hists, std::size_t *count);
int Get(hxhim_histograms_t *hists, const std::size_t idx,
        double **buckets, std::size_t **counts, std::size_t *count);
int Destroy(hxhim_histograms_t *hists);

}
}

#endif
