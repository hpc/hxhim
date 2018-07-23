/*
  This is the primary include for HXHIM, the Hexadimensional Hashing Indexing Middleware
*/

#ifndef HXHIM_H
#define HXHIM_H

#include <ctype.h>

#include "Results.h"
#include "config.h"
#include "constants.h"
#include "options.h"
#include "struct.h"
#include "utils/Histogram.h"

#ifdef __cplusplus
extern "C"
{
#endif

/** @description Starts an HXHIM instance */
int hxhimOpen(hxhim_t *hx, hxhim_options_t *opts);

/** @description Stops an HXHIM instance */
int hxhimClose(hxhim_t *hx);

/** @description Commits all flushed data on local databases to disk */
int hxhimCommit(hxhim_t *hx);

/** @description Flushes the internal statistics */
int hxhimStatFlush(hxhim_t *hx);

/** @description Flush safe HXHIM queues */
hxhim_results_t *hxhimFlushPuts(hxhim_t *hx);
hxhim_results_t *hxhimFlushGets(hxhim_t *hx);
hxhim_results_t *hxhimFlushGetOps(hxhim_t *hx);
hxhim_results_t *hxhimFlushDeletes(hxhim_t *hx);
hxhim_results_t *hxhimFlush(hxhim_t *hx);

/** @description Functions for queuing operations to perform on the underlying storage */
int hxhimPut(hxhim_t *hx,
             void *subject, size_t subject_len,
             void *predicate, size_t predicate_len,
             void *object, size_t object_len);

int hxhimGet(hxhim_t *hx,
             void *subject, size_t subject_len,
             void *predicate, size_t predicate_len);

int hxhimDelete(hxhim_t *hx,
                void *subject, size_t subject_len,
                void *predicate, size_t predicate_len);

int hxhimBPut(hxhim_t *hx,
              void **subjects, size_t *subject_lens,
              void **predicates, size_t *predicate_lens,
              void **objects, size_t *object_lens,
              size_t count);

int hxhimBGet(hxhim_t *hx,
              void **subjects, size_t *subject_lens,
              void **predicates, size_t *predicate_lens,
              size_t count);

int hxhimBGetOp(hxhim_t *hx,
                void *subject, size_t subject_len,
                void *predicate, size_t predicate_len,
                size_t num_records, enum hxhim_get_op op);

int hxhimBDelete(hxhim_t *hx,
                 void **subjects, size_t *subject_lens,
                 void **predicates, size_t *predicate_lens,
                 size_t count);

/** @description Utility Functions */
int hxhimGetStats(hxhim_t *hx, const int rank,
                  const int get_put_times, long double *put_times,
                  const int get_num_puts, size_t *num_puts,
                  const int get_get_times, long double *get_times,
                  const int get_num_gets, size_t *num_gets);

int hxhimSubjectType(hxhim_t *hx, int *type);
int hxhimPredicateType(hxhim_t *hx, int *type);
int hxhimObjectType(hxhim_t *hx, int *type);

int hxhimPutFloat(hxhim_t *hx,
                  void *subject, size_t subject_len,
                  void *predicate, size_t predicate_len,
                  float *object);

int hxhimPutDouble(hxhim_t *hx,
                   void *subject, size_t subject_len,
                   void *predicate, size_t predicate_len,
                   double *object);

int hxhimBPutFloat(hxhim_t *hx,
                   void **subjects, size_t *subject_lens,
                   void **predicates, size_t *predicate_lens,
                   float **objects,
                   size_t count);

int hxhimBPutDouble(hxhim_t *hx,
                    void **subjects, size_t *subject_lens,
                    void **predicates, size_t *predicate_lens,
                    double **objects,
                    size_t count);

int hxhimGetHistogram(hxhim_t *hx, histogram_t *histogram);

#ifdef __cplusplus
}
#endif

#endif
