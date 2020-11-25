#ifndef HXHIM_OPTIONS_HPP
#define HXHIM_OPTIONS_HPP

#include <string>

#include "hxhim/options.h"

#if HXHIM_HAVE_THALLIUM
int hxhim_options_set_transport_thallium(hxhim_options_t *opts, const std::string &module);
#endif

int hxhim_options_set_histogram_bucket_gen_name(hxhim_options_t *opts, const std::string &method);
int hxhim_options_add_histogram_track_predicate(hxhim_options_t *opts, const std::string &name);

#endif
