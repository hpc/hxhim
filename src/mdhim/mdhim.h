/*
 * MDHIM
 *
 * External API
 */

#ifndef      __MDHIM_H
#define      __MDHIM_H

#include "index_struct.h"
#include "mdhim_config.h"
#include "mdhim_constants.h"
#include "mdhim_options.h"
#include "mdhim_struct.h"
#include "transport.h"

#ifdef __cplusplus
extern "C"
{
#endif

int mdhimInit(mdhim_t *md, mdhim_options_t *opts);
int mdhimClose(mdhim_t *md);
int mdhimCommit(mdhim_t *md, index_t *index);
int mdhimStatFlush(mdhim_t *md, index_t *index);
mdhim_rm_t *mdhimPut(mdhim_t *md, index_t *index,
                     void *primary_key, size_t primary_key_len,
                     void *value, size_t value_len);
mdhim_brm_t *mdhimBPut(mdhim_t *md, index_t *index,
                       void **primary_keys, size_t *primary_key_lens,
                       void **primary_values, size_t *primary_value_lens,
                       size_t num_records);
mdhim_grm_t *mdhimGet(mdhim_t *md, index_t *index,
                      void *key, size_t key_len,
                      enum TransportGetMessageOp op);
mdhim_bgrm_t *mdhimBGet(mdhim_t *md, index_t *index,
                        void **keys, size_t *key_lens,
                        size_t num_records, enum TransportGetMessageOp op);
mdhim_bgrm_t *mdhimBGetOp(mdhim_t *md, index_t *index,
                          void *key, size_t key_len,
                          size_t num_records, enum TransportGetMessageOp op);
mdhim_rm_t *mdhimDelete(mdhim_t *md, index_t *index,
                        void *key, size_t key_len);
mdhim_brm_t *mdhimBDelete(mdhim_t *md, index_t *index,
                          void **keys, size_t *key_lens,
                          size_t num_keys);

// Utility functions
int mdhimWhichDB(mdhim_t *md, void *key, size_t key_len);
int mdhimDecomposeDB(mdhim_t *md, const int db, int *rank, int *rs_idx);
int mdhimComposeDB(mdhim_t *md, int *db, const int rank, const int rs_idx);

#ifdef __cplusplus
}
#endif
#endif
