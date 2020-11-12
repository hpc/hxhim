#ifndef HXHIM_DOUBLE_FUNCTIONS_H
#define HXHIM_DOUBLE_FUNCTIONS_H

#include <stddef.h>

#include "hxhim/constants.h"
#include "hxhim/struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

int hxhimPutDouble(hxhim_t *hx,
                   void *subject, size_t subject_len, enum hxhim_data_t subject_type,
                   void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type,
                   double *object);

int hxhimGetDouble(hxhim_t *hx,
                   void *subject, size_t subject_len, enum hxhim_data_t subject_type,
                   void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type);

int hxhimGetOpDouble(hxhim_t *hx,
                     void *subject, size_t subject_len, enum hxhim_data_t subject_type,
                     void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type,
                     size_t num_records, enum hxhim_getop_t op);

int hxhimBPutDouble(hxhim_t *hx,
                    void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                    void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                    double **objects,
                    const size_t count);

int hxhimBGetDouble(hxhim_t *hx,
                    void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                    void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                    const size_t count);

int hxhimBGetOpDouble(hxhim_t *hx,
                      void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                      void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                      size_t *num_records, enum hxhim_getop_t *ops,
                      const size_t count);

#ifdef __cplusplus
}
#endif

#endif
