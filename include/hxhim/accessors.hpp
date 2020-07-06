#ifndef HXHIM_ACCESSORS_HPP
#define HXHIM_ACCESSORS_HPP

#include <cstddef>
#include <vector>

#include <mpi.h>

#include "datastore/datastore.hpp"
#include "hxhim/constants.h"
#include "hxhim/struct.h"

namespace hxhim {

int GetMPIComm(hxhim_t *hx, MPI_Comm *comm);
int GetMPIRank(hxhim_t *hx, int *rank);
int GetMPISize(hxhim_t *hx, int *size);
int GetDatastoresPerRangeServer(hxhim_t *hx, std::size_t *datastore_count);
int GetDatastoreClientToServerRatio(hxhim_t *hx, std::size_t *client, std::size_t *server);

/** C++ only */
int GetDatastores(hxhim_t *hx, std::vector<datastore::Datastore *> **datastores);

}

#endif
