/*
  This is the primary include for HXHIM, the Hexadimensional Hashing Indexing Middleware
*/

#ifndef HXHIM_H
#define HXHIM_H

#include <ctype.h>

#include "hxhim/Results.h"
#include "hxhim/accessors.h"
#include "hxhim/config.h"
#include "hxhim/constants.h"
#include "hxhim/options.h"
#include "hxhim/struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

/** @description Starts an HXHIM instance */
int hxhimOpen(hxhim_t *hx, hxhim_options_t *opts);
int hxhimOpenOne(hxhim_t *hx, hxhim_options_t *opts, const char *db_path, const size_t db_path_len);

/** @description Stops an HXHIM instance */
int hxhimClose(hxhim_t *hx);

/** @description Functiosn for flushing HXHIM queues */
hxhim_results_t *hxhimFlushPuts(hxhim_t *hx);
hxhim_results_t *hxhimFlushGets(hxhim_t *hx);
hxhim_results_t *hxhimFlushGetOps(hxhim_t *hx);
hxhim_results_t *hxhimFlushDeletes(hxhim_t *hx);
hxhim_results_t *hxhimFlush(hxhim_t *hx);

/** @description Function that forces the datastores to flush to the underlying storage */
hxhim_results_t *hxhimSync(hxhim_t *hx);

/** @description Function that opens new datastores */
hxhim_results_t *hxhimChangeHash(hxhim_t *hx, const char *name, hxhim_hash_t func, void *args);

/** @description Functions for queuing operations to perform on the underlying storage */
int hxhimPut(hxhim_t *hx,
             void *subject, size_t subject_len,
             void *predicate, size_t predicate_len,
             enum hxhim_type_t object_type, void *object, size_t object_len);

int hxhimGet(hxhim_t *hx,
             void *subject, size_t subject_len,
             void *predicate, size_t predicate_len,
             enum hxhim_type_t object_type);

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
              enum hxhim_type_t *object_types,
              size_t count);

int hxhimBGetOp(hxhim_t *hx,
                void *subject, size_t subject_len,
                void *predicate, size_t predicate_len,
                enum hxhim_type_t object_type,
                size_t num_records, enum hxhim_get_op_t op);

int hxhimBDelete(hxhim_t *hx,
                 void **subjects, size_t *subject_lens,
                 void **predicates, size_t *predicate_lens,
                 size_t count);

/** @description Utility Functions */
int hxhimGetStats(hxhim_t *hx, const int dst_rank,
                  const int get_put_times, long double *put_times,
                  const int get_num_puts, size_t *num_puts,
                  const int get_get_times, long double *get_times,
                  const int get_num_gets, size_t *num_gets);

hxhim_results_t *hxhimGetHistogram(hxhim_t *hx, const int datastore);
hxhim_results_t *hxhimBGetHistogram(hxhim_t *hx, const int *datastores, const size_t count);

#ifdef __cplusplus
}
#endif

#include "hxhim/float.h"
#include "hxhim/double.h"
#include "hxhim/single_type.h"

#endif
