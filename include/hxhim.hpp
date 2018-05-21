#ifndef HXHIM_INTERFACE_HPP
#define HXHIM_INTERFACE_HPP

#include <string>

#include <mpi.h>

#include "hxhim-types.h"
#include "return.hpp"
#include "transport_constants.h"

namespace hxhim {

int Open(hxhim_t *hx, const MPI_Comm bootstrap_comm, const std::string &filename);
int Close(hxhim_t *hx);

int Flush(hxhim_t *hx);
Return *FlushGet(hxhim_t *hx, void *key, size_t key_len);
Return *FlushBGet(hxhim_t *hx, void **keys, size_t *key_lens, std::size_t num_keys);

int Put(hxhim_t *hx, void *key, std::size_t key_len, void *value, std::size_t value_len);
Return *Get(hxhim_t *hx, void *key, std::size_t key_len);
// int Delete(hxhim_t *hx, void *key, std::size_t key_len);

int BPut(hxhim_t *hx, void **keys, std::size_t *key_lens, void **values, std::size_t *value_lens, std::size_t num_keys);
Return *BGet(hxhim_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys);
// int BDelete(hxhim_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys);

}

#endif
