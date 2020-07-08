#ifndef HXHIM_OPTIONS_HPP
#define HXHIM_OPTIONS_HPP

#include "hxhim/options.h"
#include "utils/Histogram.h"

#if HXHIM_HAVE_THALLIUM
int hxhim_options_set_transport_thallium(hxhim_options_t *opts, const std::string &module);
#endif

int hxhim_options_set_histogram_bucket_gen_method(hxhim_options_t *opts, const HistogramBucketGenerator_t &gen, void *args);

#endif
