/*
 * MDHIM
 *
 * External API
 */

#ifndef      __MDHIM_HPP
#define      __MDHIM_HPP

#include "index_struct.h"
#include "config.hpp"
#include "constants.h"
#include "mdhim_options.h"
#include "mdhim_struct.h"
#include "transport/transport.hpp"

namespace mdhim {

int Init(mdhim_t *md, mdhim_options_t *opts);
int Close(mdhim_t *md);
int Commit(mdhim_t *md, index_t *index);
int StatFlush(mdhim_t *md, index_t *index);
TransportRecvMessage *Put(mdhim_t *md, index_t *index,
                          void *primary_key, std::size_t primary_key_len,
                          void *value, std::size_t value_len);
TransportBRecvMessage *BPut(mdhim_t *md, index_t *index,
                            void **primary_keys, std::size_t *primary_key_lens,
                            void **primary_values, std::size_t *primary_value_lens,
                            std::size_t num_keys);
TransportGetRecvMessage *Get(mdhim_t *md, index_t *index,
                             void *key, std::size_t key_len,
                             enum TransportGetMessageOp op);
TransportBGetRecvMessage *BGet(mdhim_t *md, index_t *index,
                               void **keys, std::size_t *key_lens,
                               std::size_t num_keys, enum TransportGetMessageOp op);
TransportBGetRecvMessage *BGetOp(mdhim_t *md, index_t *index,
                                 void *key, std::size_t key_len,
                                 std::size_t num_records, enum TransportGetMessageOp op);
TransportRecvMessage *Delete(mdhim_t *md, index_t *index,
                             void *key, std::size_t key_len);
TransportBRecvMessage *BDelete(mdhim_t *md, index_t *index,
                               void **keys, std::size_t *key_lens,
                               std::size_t num_keys);

namespace Unsafe {
// Functions that allow for ignoring of indexing
TransportRecvMessage *Put(mdhim_t *md, index_t *index,
                          void *primary_key, std::size_t primary_key_len,
                          void *value, std::size_t value_len,
                          const int database);
TransportBRecvMessage *BPut(mdhim_t *md, index_t *index,
                            void **primary_keys, std::size_t *primary_key_lens,
                            void **primary_values, std::size_t *primary_value_lens,
                            const int *databases,
                            std::size_t num_keys);
TransportGetRecvMessage *Get(mdhim_t *md, index_t *index,
                             void *key, std::size_t key_len,
                             const int database,
                             enum TransportGetMessageOp op);
TransportBGetRecvMessage *BGet(mdhim_t *md, index_t *index,
                               void **keys, std::size_t *key_lens,
                               const int *databases,
                               std::size_t num_keys, enum TransportGetMessageOp op);
TransportBGetRecvMessage *BGetOp(mdhim_t *md, index_t *index,
                                 void *key, size_t key_len,
                                 const int database,
                                 size_t num_records, enum TransportGetMessageOp op);
TransportRecvMessage *Delete(mdhim_t *md, index_t *index,
                             void *key, std::size_t key_len,
                             const int database);
TransportBRecvMessage *BDelete(mdhim_t *md, index_t *index,
                               void **keys, std::size_t *key_lens,
                               const int *databases,
                               std::size_t num_keys);
}

// Utility functions
int DBCount(mdhim_t *md);
int WhichDB(mdhim_t *md, void *key, std::size_t key_len);
int DecomposeDB(mdhim_t *md, const int db, int *rank, int *rs_idx);
int ComposeDB(mdhim_t *md, int *db, const int rank, const int rs_idx);
int GetStats(mdhim_t *md, const int rank,
             const bool get_put_times, long double *put_times,
             const bool get_num_puts, std::size_t *num_puts,
             const bool get_get_times, long double *get_times,
             const bool get_num_gets, std::size_t *num_gets);
}

#endif
