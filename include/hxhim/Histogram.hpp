#ifndef HXHIM_HISTOGRAM_HPP
#define HXHIM_HISTOGRAM_HPP

#include <cstddef>

#include "hxhim/Histogram.h"

namespace hxhim {
namespace histogram {

int count(hxhim_histogram_t *hists, std::size_t *count);
int get(hxhim_histogram_t *hists, const std::size_t idx,
        double **buckets, std::size_t **counts, std::size_t *count);

}
}

#endif
