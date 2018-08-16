#ifndef HXHIM_OPTIONS_HPP
#define HXHIM_OPTIONS_HPP

#include "hxhim/options.h"
#include "utils/Histogram.hpp"

int hxhim_options_set_histogram_bucket_gen_method(hxhim_options_t *opts, const Histogram::BucketGen::generator &gen, void *args);

#endif
