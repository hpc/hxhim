#ifndef HXHIM_ACCESSORS_PRIVATE_HPP
#define HXHIM_ACCESSORS_PRIVATE_HPP

#include <cstdint>

#include <mpi.h>

#include "hxhim/accessors.hpp"
#include "hxhim/hash.h"
#include "hxhim/struct.h"
#include "utils/Stats.hpp"

/**
 * These functions do not check for the validify of hx
 * and always return HXHIM_SUCCESS
 */
namespace hxhim {
namespace nocheck{

int GetEpoch(hxhim_t *hx, ::Stats::Chronopoint &epoch);
int GetMPI(hxhim_t *hx, MPI_Comm *comm, int *rank, int *size);
int GetRangeServerCount(hxhim_t *hx, std::size_t *count);
int GetRangeServerClientToServerRatio(hxhim_t *hx, std::size_t *client, std::size_t *server);
int GetDatastoreCount(hxhim_t *hx, std::size_t *count);
int GetDatastoreLocation(hxhim_t *hx, const int id, int *rank, int *offset);

int GetHash(hxhim_t *hx, const char **name, hxhim_hash_t *func, void **args);
int HaveHistogram(hxhim_t *hx, const char *name, const std::size_t name_len, int *exists);

}
}

#endif
