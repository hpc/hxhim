/*
  This is the HXHim primary include for HXHIM, the Hexadimensional Hashing Indexing Middleware
*/

#ifndef HXHIM_INTERFACE_H
#define HXHIM_INTERFACE_H

#include <ctype.h>

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "hxhim-types.h"
#include "return.h"

#ifdef __cplusplus
extern "C"
{
#endif

int hxhimOpen(hxhim_session_t *hx, const MPI_Comm bootstrap_comm, const char *filename);
int hxhimClose(hxhim_session_t *hx);

hxhim_return_t *hxhimFlush(hxhim_session_t *hx);

int hxhimPut(hxhim_session_t *hx, void *key, size_t key_len, void *value, size_t value_len);
int hxhimGet(hxhim_session_t *hx, void *key, size_t key_len);
int hxhimDelete(hxhim_session_t *hx, void *key, size_t key_len);
int hxhimBPut(hxhim_session_t *hx, void **keys, size_t *key_lens, void **values, size_t *value_lens, size_t num_keys);
int hxhimBGet(hxhim_session_t *hx, void **keys, size_t *key_lens, size_t num_keys);
int hxhimBDelete(hxhim_session_t *hx, void **keys, size_t *key_lens, size_t num_keys);

#ifdef __cplusplus
}
#endif

#endif
