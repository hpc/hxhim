/*
 * MDHIM
 *
 * External API
 */

#ifndef      __MDHIM_HPP
#define      __MDHIM_HPP

#include "index_struct.h"
#include "mdhim_config.hpp"
#include "mdhim_constants.h"
#include "mdhim_options.h"
#include "mdhim_struct.h"
#include "transport.hpp"

namespace mdhim {

int Init(mdhim_t *md, mdhim_options_t *opts);
int Close(mdhim_t *md);
int Commit(mdhim_t *md, index_t *index);
int StatFlush(mdhim_t *md, index_t *index);
TransportRecvMessage *Put(mdhim_t *md, index_t *index,
                          void *primary_key, size_t primary_key_len,
                          void *value, size_t value_len);
TransportBRecvMessage *BPut(mdhim_t *md, index_t *index,
                            void **primary_keys, size_t *primary_key_lens,
                            void **primary_values, size_t *primary_value_lens,
                            size_t num_records);
TransportGetRecvMessage *Get(mdhim_t *md, index_t *index,
                             void *key, size_t key_len,
                             enum TransportGetMessageOp op);
TransportBGetRecvMessage *BGet(mdhim_t *md, index_t *index,
                               void **keys, size_t *key_lens,
                               size_t num_records, enum TransportGetMessageOp op);
TransportBGetRecvMessage *BGetOp(mdhim_t *md, index_t *index,
                                 void *key, size_t key_len,
                                 size_t num_records, enum TransportGetMessageOp op);
TransportRecvMessage *Delete(mdhim_t *md, index_t *index,
                             void *key, size_t key_len);
TransportBRecvMessage *BDelete(mdhim_t *md, index_t *index,
                               void **keys, size_t *key_lens,
                               size_t num_keys);

// Utility functions
int WhichDB(mdhim_t *md, void *key, size_t key_len);
int DecomposeDB(mdhim_t *md, const int db, int *rank, int *rs_idx);
int ComposeDB(mdhim_t *md, int *db, const int rank, const int rs_idx);

}

#endif
