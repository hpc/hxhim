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
                  void *subject, size_t subject_len,
                  void *predicate, size_t predicate_len,
                  float *object);

int hxhimGetFloat(hxhim_t *hx,
                  void *subject, size_t subject_len,
                  void *predicate, size_t predicate_len);

int hxhimGetOpFloat(hxhim_t *hx,
                    void *subject, size_t subject_len,
                    void *predicate, size_t predicate_len,
                    size_t num_records, enum hxhim_get_op_t op);

int hxhimBPutFloat(hxhim_t *hx,
                   void **subjects, size_t *subject_lens,
                   void **predicates, size_t *predicate_lens,
                   float **objects,
                   const size_t count);

int hxhimBGetFloat(hxhim_t *hx,
                   void **subjects, size_t *subject_lens,
                   void **predicates, size_t *predicate_lens,
                   const size_t count);

int hxhimBGetOpFloat(hxhim_t *hx,
                     void **subjects, size_t *subject_lens,
                     void **predicates, size_t *predicate_lens,
                     size_t *num_records, enum hxhim_get_op_t *ops,
                     const size_t count);

#ifdef __cplusplus
}
#endif

#endif
