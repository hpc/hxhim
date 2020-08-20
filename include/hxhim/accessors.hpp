#ifndef HXHIM_ACCESSORS_HPP
#define HXHIM_ACCESSORS_HPP

#include <cstddef>
#include <ctime>
#include <ostream>

#include <mpi.h>

#include "hxhim/accessors.h"
#include "hxhim/struct.h"
#include "utils/Stats.hpp"

namespace hxhim {

int GetEpoch(hxhim_t *hx, struct timespec *epoch);
int GetEpoch(hxhim_t *hx, ::Stats::Chronopoint &epoch);
int GetMPI(hxhim_t *hx, MPI_Comm *comm, int *rank, int *size);
int GetDatastoresPerRangeServer(hxhim_t *hx, std::size_t *datastore_count);
int GetDatastoreCount(hxhim_t *hx, std::size_t *count);
int GetDatastoreClientToServerRatio(hxhim_t *hx, std::size_t *client, std::size_t *server);

int GetPrintBufferContents(hxhim_t *hx, std::ostream &stream);

}

#endif
