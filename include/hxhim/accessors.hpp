#ifndef HXHIM_ACCESSORS_HPP
#define HXHIM_ACCESSORS_HPP

#include <cstddef>

#include <mpi.h>

#include "hxhim/struct.h"

namespace hxhim {

int GetMPI(hxhim_t *hx, MPI_Comm *comm, int *rank, int *size);
int GetDatastoresPerRangeServer(hxhim_t *hx, std::size_t *datastore_count);
int GetDatastoreCount(hxhim_t *hx, std::size_t *count);
int GetDatastoreClientToServerRatio(hxhim_t *hx, std::size_t *client, std::size_t *server);

}

#endif
