#ifndef HXHIM_INTERFACE_HPP
#define HXHIM_INTERFACE_HPP

#include <string>

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "hxhim-types.h"
#include "return.hpp"

namespace hxhim {

int open(hxhim_session_t *hx, const MPI_Comm bootstrap_comm, const std::string &filename);
int close(hxhim_session_t *hx);

Return *flush(hxhim_session_t *hx);

int put(hxhim_session_t *hx, void *key, std::size_t key_len, void *value, std::size_t value_len);
int get(hxhim_session_t *hx, void *key, std::size_t key_len);
int del(hxhim_session_t *hx, void *key, std::size_t key_len);
int bput(hxhim_session_t *hx, void **keys, std::size_t *key_lens, void **values, std::size_t *value_lens, std::size_t num_keys);
int bget(hxhim_session_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys);
int bdel(hxhim_session_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys);

}

#endif
