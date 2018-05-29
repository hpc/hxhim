/*
  This is the primary include for HXHIM, the Hexadimensional Hashing Indexing Middleware
*/

#ifndef HXHIM_H
#define HXHIM_H

#include <ctype.h>

#include <mpi.h>

#include "hxhim-types.h"
#include "return.h"
#include "transport_constants.h"

#ifdef __cplusplus
extern "C"
{
#endif

/** @description Starts an HXHIM instance */
int hxhimOpen(hxhim_t *hx, const MPI_Comm bootstrap_comm);

/** @description Stops an HXHIM instance */
int hxhimClose(hxhim_t *hx);

/** @description Flush safe and unsafe HXHIM queues */
hxhim_return_t *hxhimFlushAllPuts(hxhim_t *hx);
hxhim_return_t *hxhimFlushAllGets(hxhim_t *hx);
hxhim_return_t *hxhimFlushAllGetOps(hxhim_t *hx);
hxhim_return_t *hxhimFlushAllDeletes(hxhim_t *hx);
hxhim_return_t *hxhimFlushAll(hxhim_t *hx);

/** @description Flush safe HXHIM queues */
hxhim_return_t *hxhimFlushPuts(hxhim_t *hx);
hxhim_return_t *hxhimFlushGets(hxhim_t *hx);
hxhim_return_t *hxhimFlushGetOps(hxhim_t *hx);
hxhim_return_t *hxhimFlushDeletes(hxhim_t *hx);
hxhim_return_t *hxhimFlush(hxhim_t *hx);

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
                size_t num_records, enum TransportGetMessageOp op);

int hxhimBDelete(hxhim_t *hx,
                 void **subjects, size_t *subject_lens,
                 void **predicates, size_t *predicate_lens,
                 size_t count);

int hxhimGetStats(hxhim_t *hx, const int rank,
                  const int get_put_times, long double *put_times,
                  const int get_num_puts, size_t *num_puts,
                  const int get_get_times, long double *get_times,
                  const int get_num_gets, size_t *num_gets);

/** @description Analagous to their standard function counterparts,
    with the ability to send keys to user specified databases,
    rather than the database determined by the indexing */
hxhim_return_t *hxhimFlushUnsafePuts(hxhim_t *hx);
hxhim_return_t *hxhimFlushUnsafeGets(hxhim_t *hx);
hxhim_return_t *hxhimFlushUnsafeGetOps(hxhim_t *hx);
hxhim_return_t *hxhimFlushUnsafeDeletes(hxhim_t *hx);
hxhim_return_t *hxhimFlushUnsafe(hxhim_t *hx);

int hxhimUnsafePut(hxhim_t *hx,
                   void *subject, size_t subject_len,
                   void *predicate, size_t predicate_len,
                   void *object, size_t object_len,
                   const int database);

int hxhimUnsafeGet(hxhim_t *hx,
                   void *subject, size_t subject_len,
                   void *predicate, size_t predicate_len,
                   const int database);

int hxhimUnsafeDelete(hxhim_t *hx,
                      void *subject, size_t subject_len,
                      void *predicate, size_t predicate_len,
                      const int database);

int hxhimUnsafeBPut(hxhim_t *hx,
                    void **subjects, size_t *subject_lens,
                    void **predicates, size_t *predicate_lens,
                    void **objects, size_t *object_lens,
                    const int *databases, size_t count);

int hxhimUnsafeBGet(hxhim_t *hx,
                    void **subjects, size_t *subject_lens,
                    void **predicates, size_t *predicate_lens,
                    const int *databases, size_t count);

int hxhimUnsafeBGetOp(hxhim_t *hx,
                      void *subject, size_t subject_len,
                      void *predicate, size_t predicate_len,
                      size_t num_records, enum TransportGetMessageOp op,
                      const int database);

int hxhimUnsafeBDelete(hxhim_t *hx,
                       void **subjects, size_t *subject_lens,
                       void **predicates, size_t *predicate_lens,
                       const int *databases, size_t count);

#ifdef __cplusplus
}
#endif

#endif
