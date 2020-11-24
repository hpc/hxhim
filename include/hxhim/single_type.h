#ifndef HXHIM_BULK_DATA_SINGLE_TYPE_H
#define HXHIM_BULK_DATA_SINGLE_TYPE_H

#include <stddef.h>

#include "hxhim/constants.h"
#include "hxhim/struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

int hxhimBPutSingleType(hxhim_t *hx,
                        void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                        void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                        void **objects, size_t *object_lens, enum hxhim_data_t object_type,
                        const hxhim_put_permutation_t *permutations,
                        const size_t count);

int hxhimBGetSingleType(hxhim_t *hx,
                        void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                        void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                        enum hxhim_data_t object_type,
                        const size_t count);

int hxhimBGetOpSingleType(hxhim_t *hx,
                          void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                          void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                          enum hxhim_data_t object_type,
                          size_t *num_records, enum hxhim_getop_t *ops,
                          const size_t count);

#ifdef __cplusplus
}
#endif

#endif
