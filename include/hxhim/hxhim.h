/*
  This is the primary include for HXHIM, the Hexadimensional Hashing Indexing Middleware
*/

#ifndef HXHIM_H
#define HXHIM_H

#include <stddef.h>

#include <mpi.h>

#include "hxhim/Results.h"
#include "hxhim/accessors.h"
#include "hxhim/constants.h"
#include "hxhim/double.h"
#include "hxhim/float.h"
#include "hxhim/options.h"
#include "hxhim/single_type.h"
#include "hxhim/struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

/** @description allocate an HXHIM struct */
int hxhimInit(hxhim_t *hx, MPI_Comm comm);

/** @description Starts an HXHIM instance */
int hxhimOpen(hxhim_t *hx);
int hxhimOpenOne(hxhim_t *hx, const char *db_path, const size_t db_path_len);

/** @description Stops an HXHIM instance */
int hxhimClose(hxhim_t *hx);

/** @description Functiosn for flushing HXHIM queues */
hxhim_results_t *hxhimFlushPuts(hxhim_t *hx);
hxhim_results_t *hxhimFlushGets(hxhim_t *hx);
hxhim_results_t *hxhimFlushGetOps(hxhim_t *hx);
hxhim_results_t *hxhimFlushDeletes(hxhim_t *hx);
hxhim_results_t *hxhimFlushHistograms(hxhim_t *hx);
hxhim_results_t *hxhimFlush(hxhim_t *hx);

/** @description Function that forces the datastores to flush to the underlying storage */
hxhim_results_t *hxhimSync(hxhim_t *hx);

/** @description Function that opens new datastores */
hxhim_results_t *hxhimChangeHash(hxhim_t *hx, const char *name, const size_t name_len,
                                 hxhim_hash_t func, void *args);

/** @description Function for changing the datastore name without changing the hash */
hxhim_results_t *hxhimChangeDatastoreName(hxhim_t *hx, const char *basename, const size_t basename_len,
                                          const int write_histograms, const int read_histograms,
                                          const int create_missing);

/** @description Functions for queuing operations to perform on the underlying storage */
int hxhimPut(hxhim_t *hx,
             void *subject, size_t subject_len, enum hxhim_data_t subject_type,
             void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type,
             void *object, size_t object_len, enum hxhim_data_t object_type,
             const hxhim_put_permutation_t permutations);

int hxhimGet(hxhim_t *hx,
             void *subject, size_t subject_len, enum hxhim_data_t subject_type,
             void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type,
             enum hxhim_data_t object_type);

int hxhimGetOp(hxhim_t *hx,
               void *subject, size_t subject_len, enum hxhim_data_t subject_type,
               void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type,
               enum hxhim_data_t object_type,
               size_t num_records, enum hxhim_getop_t op);

int hxhimDelete(hxhim_t *hx,
                void *subject, size_t subject_len, enum hxhim_data_t subject_type,
                void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type);

int hxhimBPut(hxhim_t *hx,
              void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
              void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
              void **objects, size_t *object_lens, enum hxhim_data_t *object_types,
              const hxhim_put_permutation_t *permutations,
              const size_t count);

int hxhimBGet(hxhim_t *hx,
              void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
              void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
              enum hxhim_data_t *object_types,
              const size_t count);

int hxhimBGetOp(hxhim_t *hx,
                void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                enum hxhim_data_t *object_types,
                size_t *num_records, enum hxhim_getop_t *ops,
                const size_t count);

int hxhimBDelete(hxhim_t *hx,
                 void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                 void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                 const size_t count);

// /** @description Utility Functions */
// int hxhimGetStats(hxhim_t *hx, const int dst_rank,
//                   long double *put_times,
//                   size_t *num_puts,
//                   long double *get_times,
//                   size_t *num_gets);

int hxhimHistogram(hxhim_t *hx,
                   int rs_id,
                   const char *name, const size_t name_len);

int hxhimBHistogram(hxhim_t *hx,
                    int *rs_ids,
                    const char **names, const size_t *name_lens,
                    const size_t count);

// int hxhimGetMinFilled(hxhim_t *hx, const int dst_rank,
//                       const int get_bput, long double *bput,
//                       const int get_bget, long double *bget,
//                       const int get_bgetop, long double *bgetop,
//                       const int get_bdel, long double *bdel);

// int hxhimGetAverageFilled(hxhim_t *hx, const int dst_rank,
//                           const int get_bput, long double *bput,
//                           const int get_bget, long double *bget,
//                           const int get_bgetop, long double *bgetop,
//                           const int get_bdel, long double *bdel);

// int hxhimGetMaxFilled(hxhim_t *hx, const int dst_rank,
//                       const int get_bput, long double *bput,
//                       const int get_bget, long double *bget,
//                       const int get_bgetop, long double *bgetop,
//                       const int get_bdel, long double *bdel);

#ifdef __cplusplus
}
#endif

#endif
