#ifndef HXHIM_FLOAT_FUNCTIONS_H
#define HXHIM_FLOAT_FUNCTIONS_H

#include "hxhim/constants.h"
#include "hxhim/struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

int hxhimPutDouble(hxhim_t *hx,
                   void *subject, size_t subject_len,
                   void *predicate, size_t predicate_len,
                   double *object);

int hxhimGetDouble(hxhim_t *hx,
                   void *subject, size_t subject_len,
                   void *predicate, size_t predicate_len);

int hxhimBPutDouble(hxhim_t *hx,
                    void **subjects, size_t *subject_lens,
                    void **predicates, size_t *predicate_lens,
                    double **objects,
                    size_t count);

int hxhimBGetDouble(hxhim_t *hx,
                    void **subjects, size_t *subject_lens,
                    void **predicates, size_t *predicate_lens,
                    size_t count);

int hxhimBGetOpDouble(hxhim_t *hx,
                      void *subject, size_t subject_len,
                      void *predicate, size_t predicate_len,
                      size_t num_records, enum hxhim_get_op_t op);

#ifdef __cplusplus
}
#endif

#endif
