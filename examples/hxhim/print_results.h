#ifndef HXHIM_EXAMPLES_PRINT_RESULTS
#define HXHIM_EXAMPLES_PRINT_RESULTS

#include "hxhim/hxhim.h"

#ifdef __cplusplus
extern "C"
{
#endif

void print_by_type(void *value, size_t value_len, enum hxhim_data_t type);
void print_results(hxhim_t *hx, const int print_rank, hxhim_results_t *results);

#ifdef __cplusplus
}
#endif

#endif
