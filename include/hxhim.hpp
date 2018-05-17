#ifndef HXHIM_INTERFACE_HPP
#define HXHIM_INTERFACE_HPP

#include <string>

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "hxhim-types.h"
#include "return.hpp"

namespace hxhim {

int Open(hxhim_session_t *hx, const MPI_Comm bootstrap_comm, const std::string &filename);
int Close(hxhim_session_t *hx);

Return *Flush(hxhim_session_t *hx);

int Put(hxhim_session_t *hx, void *key, std::size_t key_len, void *value, std::size_t value_len);
int Get(hxhim_session_t *hx, void *key, std::size_t key_len);
int Delete(hxhim_session_t *hx, void *key, std::size_t key_len);
int BPut(hxhim_session_t *hx, void **keys, std::size_t *key_lens, void **values, std::size_t *value_lens, std::size_t num_keys);
int BGet(hxhim_session_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys);
int BDelete(hxhim_session_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys);

}

#endif
