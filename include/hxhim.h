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

int hxhimOpen(hxhim_t *hx, const MPI_Comm bootstrap_comm, const char *filename);
int hxhimClose(hxhim_t *hx);

int hxhimFlush(hxhim_t *hx);

int hxhimPut(hxhim_t *hx, void *key, size_t key_len, void *value, size_t value_len);
hxhim_return_t *hxhimGet(hxhim_t *hx, void *key, size_t key_len);
// int hxhimDelete(hxhim_t *hx, void *key, size_t key_len);
int hxhimBPut(hxhim_t *hx, void **keys, size_t *key_lens, void **values, size_t *value_lens, size_t num_keys);
hxhim_return_t *hxhimBGet(hxhim_t *hx, void **keys, size_t *key_lens, size_t num_keys);
// int hxhimBDelete(hxhim_t *hx, void **keys, size_t *key_lens, size_t num_keys);

#ifdef __cplusplus
}
#endif

#endif
