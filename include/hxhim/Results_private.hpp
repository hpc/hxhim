#ifndef HXHIM_RESULTS_PRIVATE_HPP
#define HXHIM_RESULTS_PRIVATE_HPP

#include "hxhim/Results.hpp"
#include "hxhim/struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_results {
    hxhim::Results *res;
} hxhim_results_t;

#ifdef __cplusplus
}
#endif

hxhim_results_t *hxhim_results_init(hxhim_t *hx, hxhim::Results *res);

#endif
