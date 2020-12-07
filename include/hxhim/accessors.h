#ifndef HXHIM_ACCESSORS_H
#define HXHIM_ACCESSORS_H

#include <stddef.h>
#include <time.h>

#include <mpi.h>

#include "hxhim/constants.h"
#include "hxhim/hash.h"
#include "hxhim/struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

int hxhimGetEpoch(hxhim_t *hx, struct timespec *epoch);
int hxhimGetMPI(hxhim_t *hx, MPI_Comm *comm, int *rank, int *size);
int hxhimGetRangeServerCount(hxhim_t *hx, size_t *count);
int hxhimGetRangeServerClientToServerRatio(hxhim_t *hx, size_t *client, size_t *server);

int hxhimGetHash(hxhim_t *hx, const char **name, hxhim_hash_t *func, void **args);
int hxhimGetDatastorePrefix(hxhim_t *hx, const char **prefix, size_t *prefix_len);
int hxhimHaveHistogram(hxhim_t *hx, const char *name, const size_t name_len, int *exists);

#ifdef __cplusplus
}
#endif

#endif
