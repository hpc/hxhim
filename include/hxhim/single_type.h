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
                        void **subjects, size_t *subject_lens,
                        void **predicates, size_t *predicate_lens,
                        enum hxhim_object_type_t object_type, void **objects, size_t *object_lens,
                        const size_t count);

int hxhimBGetSingleType(hxhim_t *hx,
                        void **subjects, size_t *subject_lens,
                        void **predicates, size_t *predicate_lens,
                        enum hxhim_object_type_t object_type,
                        const size_t count);

int hxhimBGetOpSingleType(hxhim_t *hx,
                          void **subjects, size_t *subject_lens,
                          void **predicates, size_t *predicate_lens,
                          enum hxhim_object_type_t object_type,
                          size_t *num_records, enum hxhim_get_op_t *ops,
                          const size_t count);

#ifdef __cplusplus
}
#endif

#endif
