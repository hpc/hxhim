#ifndef HXHIM_FLOAT_FUNCTIONS_H
#define HXHIM_FLOAT_FUNCTIONS_H

#include <stddef.h>

#include "hxhim/constants.h"
#include "hxhim/struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

int hxhimPutFloat(hxhim_t *hx,
                  void *subject, size_t subject_len, enum hxhim_data_t subject_type,
                  void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type,
                  float *object,
                  const hxhim_put_permutation_t permutations);

int hxhimGetFloat(hxhim_t *hx,
                  void *subject, size_t subject_len, enum hxhim_data_t subject_type,
                  void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type);

int hxhimGetOpFloat(hxhim_t *hx,
                    void *subject, size_t subject_len, enum hxhim_data_t subject_type,
                    void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type,
                    size_t num_records, enum hxhim_getop_t op);

int hxhimBPutFloat(hxhim_t *hx,
                   void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                   void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                   float **objects,
                   const hxhim_put_permutation_t *permutations,
                   const size_t count);

int hxhimBGetFloat(hxhim_t *hx,
                   void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                   void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                   const size_t count);

int hxhimBGetOpFloat(hxhim_t *hx,
                     void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                     void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                     size_t *num_records, enum hxhim_getop_t *ops,
                     const size_t count);

#ifdef __cplusplus
}
#endif

#endif
