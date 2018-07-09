/*
  This is the primary include for HXHIM, the Hexadimensional Hashing Indexing Middleware
*/

#ifndef HXHIM_H
#define HXHIM_H

#include <ctype.h>

#include <mpi.h>

#include "constants.h"
#include "struct.h"
#include "Results.h"

#ifdef __cplusplus
extern "C"
{
#endif

/** @description Starts an HXHIM instance */
int hxhimOpen(hxhim_t *hx, const MPI_Comm bootstrap_comm);

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

/** @description Standard functions for storing and retrieving records */
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

int hxhimGetStats(hxhim_t *hx, const int rank,
                  const int get_put_times, long double *put_times,
                  const int get_num_puts, size_t *num_puts,
                  const int get_get_times, long double *get_times,
                  const int get_num_gets, size_t *num_gets);

#ifdef __cplusplus
}
#endif

#endif