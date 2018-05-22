/*
  This is the HXHim primary include for HXHIM, the Hexadimensional Hashing Indexing Middleware
*/

#ifndef HXHIM_INTERFACE_H
#define HXHIM_INTERFACE_H

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

/** @description Flush individual HXHIM queues */
hxhim_return_t *hxhimFlushPuts(hxhim_t *hx);
hxhim_return_t *hxhimFlushGets(hxhim_t *hx);
hxhim_return_t *hxhimFlushDeletes(hxhim_t *hx);

/** @description Flush all HXHIM queues */
hxhim_return_t **hxhimFlush(hxhim_t *hx);
void hxhimDestroyFlush(hxhim_return_t **ret);

/** @description Add single sets of data to the HXHIM queues */
int hxhimPut(hxhim_t *hx, void *key, size_t key_len, void *value, size_t value_len);
int hxhimGet(hxhim_t *hx, void *key, size_t key_len);
int hxhimDelete(hxhim_t *hx, void *key, size_t key_len);

/** @description Add multiple sets of data to the HXHIM queues */
int hxhimBPut(hxhim_t *hx, void **keys, size_t *key_lens, void **values, size_t *value_lens, size_t num_keys);
int hxhimBGet(hxhim_t *hx, void **keys, size_t *key_lens, size_t num_keys);
int hxhimBDelete(hxhim_t *hx, void **keys, size_t *key_lens, size_t num_keys);

#ifdef __cplusplus
}
#endif

#endif
