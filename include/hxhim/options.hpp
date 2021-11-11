#ifndef HXHIM_OPTIONS_HPP
#define HXHIM_OPTIONS_HPP

#include <string>

#include "hxhim/options.h"

#if HXHIM_HAVE_THALLIUM
int hxhim_set_transport_thallium(hxhim_t *hx, const std::string &module);
#endif

int hxhim_set_histogram_bucket_gen_name(hxhim_t *hx, const std::string &method);
int hxhim_add_histogram_track_predicate(hxhim_t *hx, const std::string &name);

#endif
