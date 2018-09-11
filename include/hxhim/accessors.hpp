#ifndef HXHIM_ACCESSORS_HPP
#define HXHIM_ACCESSORS_HPP

#include <cstddef>

#include <mpi.h>

#include "hxhim/constants.h"
#include "hxhim/struct.h"

namespace hxhim {

int GetMPIComm(hxhim_t *hx, MPI_Comm *comm);
int GetMPIRank(hxhim_t *hx, int *rank);
int GetMPISize(hxhim_t *hx, int *size);
int GetDatastoreCount(hxhim_t *hx, std::size_t *datastore_count);

}

#endif