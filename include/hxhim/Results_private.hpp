#ifndef HXHIM_RESULTS_PREIVATE_HPP
#define HXHIM_RESULTS_PREIVATE_HPP

#include "hxhim/Results.hpp"

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

hxhim_results_t *hxhim_results_init(hxhim::Results *res);

#endif
