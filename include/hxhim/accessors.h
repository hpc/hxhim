#ifndef HXHIM_ACCESSORS_H
#define HXHIM_ACCESSORS_H

#include <stddef.h>

#include <mpi.h>

#include "hxhim/constants.h"
#include "hxhim/struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

int hxhimGetMPIComm(hxhim_t *hx, MPI_Comm *comm);
int hxhimGetMPIRank(hxhim_t *hx, int *rank);
int hxhimGetMPISize(hxhim_t *hx, int *size);
int hxhimGetDatastoresPerRangeServer(hxhim_t *hx, size_t *datastore_count);
int hxhimGetDatastoreClientToServerRatio(hxhim_t *hx, size_t *client, size_t *server);

#ifdef __cplusplus
}
#endif

#endif
