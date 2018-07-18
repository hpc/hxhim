/*
 * MDHIM
 *
 * External API
 */

#ifndef      __MDHIM_H
#define      __MDHIM_H

#include "config.h"
#include "constants.h"
#include "index_struct.h"
#include "options.h"
#include "struct.h"
#include "transport/transport.h"

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
                       size_t num_keys);
mdhim_grm_t *mdhimGet(mdhim_t *md, index_t *index,
                      void *key, size_t key_len,
                      enum TransportGetMessageOp op);
mdhim_bgrm_t *mdhimBGet(mdhim_t *md, index_t *index,
                        void **keys, size_t *key_lens,
                        size_t num_keys, enum TransportGetMessageOp op);
mdhim_bgrm_t *mdhimBGetOp(mdhim_t *md, index_t *index,
                          void *key, size_t key_len,
                          size_t num_records, enum TransportGetMessageOp op);
mdhim_rm_t *mdhimDelete(mdhim_t *md, index_t *index,
                        void *key, size_t key_len);
mdhim_brm_t *mdhimBDelete(mdhim_t *md, index_t *index,
                          void **keys, size_t *key_lens,
                          size_t num_keys);

// Functions that allow for ignoring of indexing
mdhim_rm_t *mdhimUnsafePut(mdhim_t *md, index_t *index,
                           void *primary_key, size_t primary_key_len,
                           void *value, size_t value_len,
                           const int database);
mdhim_brm_t *mdhimUnsafeBPut(mdhim_t *md, index_t *index,
                             void **primary_keys, size_t *primary_key_lens,
                             void **primary_values, size_t *primary_value_lens,
                             const int *databases,
                             size_t num_keys);
mdhim_grm_t *mdhimUnsafeGet(mdhim_t *md, index_t *index,
                            void *key, size_t key_len,
                            const int database,
                            enum TransportGetMessageOp op);
mdhim_bgrm_t *mdhimUnsafeBGet(mdhim_t *md, index_t *index,
                              void **keys, size_t *key_lens,
                              const int *databases,
                              size_t num_keys, enum TransportGetMessageOp op);
mdhim_bgrm_t *mdhimUnsafeBGetOp(mdhim_t *md, index_t *index,
                                void *key, size_t key_len,
                                const int database,
                                size_t num_records, enum TransportGetMessageOp op);
mdhim_rm_t *mdhimUnsafeDelete(mdhim_t *md, index_t *index,
                              void *key, size_t key_len,
                              const int database);
mdhim_brm_t *mdhimUnsafeBDelete(mdhim_t *md, index_t *index,
                                void **keys, size_t *key_lens,
                                const int *databases,
                                size_t num_keys);

// Utility functions
int mdhimDBCount(mdhim_t *md);
int mdhimWhichDB(mdhim_t *md, void *key, size_t key_len);
int mdhimDecomposeDB(mdhim_t *md, const int db, int *rank, int *rs_idx);
int mdhimComposeDB(mdhim_t *md, int *db, const int rank, const int rs_idx);
int mdhimGetStats(mdhim_t *md, const int rank,
                  const int get_put_times, long double *put_times,
                  const int get_num_puts, size_t *num_puts,
                  const int get_get_times, long double *get_times,
                  const int get_num_gets, size_t *num_gets);

#ifdef __cplusplus
}
#endif
#endif
