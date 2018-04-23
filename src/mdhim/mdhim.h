/*
 * MDHIM TNG
 *
 * External API and data structures
 */

#ifndef      __MDHIM_H
#define      __MDHIM_H

#include "mlog2.h"
#include "mlogfacs2.h"

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
                      void *primary_key, int primary_key_len,
                      void *value, int value_len);
mdhim_brm_t *mdhimBPut(mdhim_t *md, index_t *index,
                       void **primary_keys, int *primary_key_lens,
                       void **primary_values, int *primary_value_lens,
                       int num_records);
mdhim_getrm_t *mdhimGet(mdhim_t *md, index_t *index,
                        void *key, int key_len,
                        enum TransportGetMessageOp op);
mdhim_bgetrm_t *mdhimBGet(mdhim_t *md, index_t *index,
                          void **keys, int *key_lens,
                          int num_records, enum TransportGetMessageOp op);
mdhim_bgetrm_t *mdhimBGetOp(mdhim_t *md, index_t *index,
                            void *key, int key_len,
                            int num_records, enum TransportGetMessageOp op);
mdhim_rm_t *mdhimDelete(mdhim_t *md, index_t *index,
                         void *key, int key_len);
mdhim_brm_t *mdhimBDelete(mdhim_t *md, index_t *index,
                          void **keys, int *key_lens,
                          int num_keys);

int mdhimWhichServer(mdhim_t *md, void *key, int key_len);

#ifdef __cplusplus
}
#endif
#endif
