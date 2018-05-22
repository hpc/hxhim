#ifndef HXHIM_INTERFACE_HPP
#define HXHIM_INTERFACE_HPP

#include <string>

#include <mpi.h>

#include "hxhim-types.h"
#include "return.hpp"
#include "transport_constants.h"

namespace hxhim {

/** @description Starts an HXHIM instance */
int Open(hxhim_t *hx, const MPI_Comm bootstrap_comm, const std::string &filename);

/** @description Stops an HXHIM instance */
int Close(hxhim_t *hx);

/** @description Flush individual HXHIM queues */
Return *FlushPuts(hxhim_t *hx);
Return *FlushGets(hxhim_t *hx);
Return *FlushDeletes(hxhim_t *hx);

/** @description Flush all HXHIM queues */
Return **Flush(hxhim_t *hx);
void DestroyFlush(Return **res);

/** @description Add single sets of data to the HXHIM queues */
int Put(hxhim_t *hx, void *key, std::size_t key_len, void *value, std::size_t value_len);
int Get(hxhim_t *hx, void *key, std::size_t key_len);
int Delete(hxhim_t *hx, void *key, std::size_t key_len);

/** @description Add multiple sets of data to the HXHIM queues */
int BPut(hxhim_t *hx, void **keys, std::size_t *key_lens, void **values, std::size_t *value_lens, std::size_t num_keys);
int BGet(hxhim_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys);
int BDelete(hxhim_t *hx, void **keys, std::size_t *key_lens, std::size_t num_keys);

}

#endif
