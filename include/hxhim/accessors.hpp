#ifndef HXHIM_ACCESSORS_HPP
#define HXHIM_ACCESSORS_HPP

#include <cstddef>
#include <ctime>
#include <ostream>

#include <mpi.h>

#include "hxhim/accessors.h"
#include "hxhim/hash.h"
#include "hxhim/struct.h"
#include "utils/Stats.hpp"

namespace hxhim {

int GetEpoch(hxhim_t *hx, struct timespec *epoch);
int GetEpoch(hxhim_t *hx, ::Stats::Chronopoint &epoch);
int GetMPI(hxhim_t *hx, MPI_Comm *comm, int *rank, int *size);
int GetRangeServerCount(hxhim_t *hx, std::size_t *count);
int GetRangeServerClientToServerRatio(hxhim_t *hx, std::size_t *client, std::size_t *server);
int GetDatastoreCount(hxhim_t *hx, std::size_t *count);
int GetDatastoreLocation(hxhim_t *hx, const int id, int *rank, int *offset);

int GetPrintBufferContents(hxhim_t *hx, std::ostream &stream);

int GetHash(hxhim_t *hx, const char **name, hxhim_hash_t *func, void **args);
int HaveHistogram(hxhim_t *hx, const char *name, const std::size_t name_len, int *exists);

}

#endif
