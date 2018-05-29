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
int hxhimOpen(hxhim_t *hx, const MPI_Comm bootstrap_comm, const char *filename);

/** @description Stops an HXHIM instance */
int hxhimClose(hxhim_t *hx);

/** @description Flush safe and unsafe HXHIM queues */
hxhim_return_t *hxhimFlushAllPuts(hxhim_t *hx);
hxhim_return_t *hxhimFlushAllGets(hxhim_t *hx);
hxhim_return_t *hxhimFlushAllDeletes(hxhim_t *hx);
hxhim_return_t *hxhimFlushAll(hxhim_t *hx);

/** @description Flush safe HXHIM queues */
hxhim_return_t *hxhimFlushPuts(hxhim_t *hx);
hxhim_return_t *hxhimFlushGets(hxhim_t *hx);
hxhim_return_t *hxhimFlushDeletes(hxhim_t *hx);
hxhim_return_t *hxhimFlush(hxhim_t *hx);

int hxhimPut(hxhim_t *hx, void *key, size_t key_len, void *value, size_t value_len);
int hxhimGet(hxhim_t *hx, void *key, size_t key_len);
int hxhimDelete(hxhim_t *hx, void *key, size_t key_len);
int hxhimBPut(hxhim_t *hx, void **keys, size_t *key_lens, void **values, size_t *value_lens, size_t num_keys);
int hxhimBGet(hxhim_t *hx, void **keys, size_t *key_lens, size_t num_keys);
int hxhimBDelete(hxhim_t *hx, void **keys, size_t *key_lens, size_t num_keys);

/** @description Flush unsafe HXHIM queues */
hxhim_return_t *hxhimFlushUnsafePuts(hxhim_t *hx);
hxhim_return_t *hxhimFlushUnsafeGets(hxhim_t *hx);
hxhim_return_t *hxhimFlushUnsafeDeletes(hxhim_t *hx);
hxhim_return_t *hxhimFlushUnsafe(hxhim_t *hx);

/** @description Functions that allow for ignoring of indexing */
int hxhimUnsafePut(hxhim_t *hx, void *key, size_t key_len, void *value, size_t value_len, const int database);
int hxhimUnsafeGet(hxhim_t *hx, void *key, size_t key_len, const int database);
int hxhimUnsafeDelete(hxhim_t *hx, void *key, size_t key_len, const int database);
int hxhimUnsafeBPut(hxhim_t *hx, void **keys, size_t *key_lens, void **values, size_t *value_lens, const int *databases, size_t num_keys);
int hxhimUnsafeBGet(hxhim_t *hx, void **keys, size_t *key_lens, const int *databases, size_t num_keys);
int hxhimUnsafeBDelete(hxhim_t *hx, void **keys, size_t *key_lens, const int *databases, size_t num_keys);

#ifdef __cplusplus
}
#endif

#endif
