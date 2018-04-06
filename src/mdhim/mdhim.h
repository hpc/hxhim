/*
 * MDHIM TNG
 *
 * External API and data structures
 */

#ifndef      __MDHIM_H
#define      __MDHIM_H

#include <pthread.h>
#include <stdint.h>

#include "mlog2.h"
#include "mlogfacs2.h"

#include "indexes.h"
#include "mdhim_constants.h"
#include "mdhim_options.h"
#include "mdhim_struct.h"
#include "secondary_info.h"
#include "transport.h"

#ifdef __cplusplus
extern "C"
{
#endif

int mdhimInit(mdhim_t *md, mdhim_options_t *opts);
int mdhimClose(mdhim_t *md);
int mdhimCommit(mdhim_t *md, struct index *index);
int mdhimStatFlush(mdhim_t *md, struct index *index);
mdhim_brm_t *mdhimPut(mdhim_t *md,
                      void *primary_key, int primary_key_len,
                      void *value, int value_len,
                      secondary_info_t *secondary_global_info,
                      secondary_info_t *secondary_local_info);
// mdhim_brm_t *mdhimPutSecondary(mdhim_t *md,
//                                struct index *secondary_index,
//                                void *secondary_key, int secondary_key_len,
//                                void *primary_key, int primary_key_len);
// mdhim_brm_t *mdhimBPut(mdhim_t *md,
//                        void **primary_keys, int *primary_key_lens,
//                        void **primary_values, int *primary_value_lens,
//                        int num_records,
//                        secondary_bulk_info_t *secondary_global_info,
//                        secondary_bulk_info_t *secondary_local_info);
mdhim_getrm_t *mdhimGet(mdhim_t *md, struct index *index,
                        void *key, int key_len,
                        enum TransportGetMessageOp op);
// mdhim_bgetrm_t *mdhimBGet(mdhim_t *md, struct index *index,
//                           void **keys, int *key_lens,
//                           int num_records, int op);
// mdhim_bgetrm_t *mdhimBGetOp(mdhim_t *md, struct index *index,
//                             void *key, int key_len,
//                             int num_records, int op);
// mdhim_brm_t *mdhimDelete(mdhim_t *md, struct index *index,
//                          void *key, int key_len);
// mdhim_brm_t *mdhimBDelete(mdhim_t *md, struct index *index,
//                           void **keys, int *key_lens,
//                           int num_keys);
secondary_info_t *mdhimCreateSecondaryInfo(struct index *secondary_index,
                                           void **secondary_keys, int *secondary_key_lens,
                                           int num_keys, int info_type);

void mdhimReleaseSecondaryInfo(secondary_info_t *si);
secondary_bulk_info_t *mdhimCreateSecondaryBulkInfo(struct index *secondary_index,
                                                    void ***secondary_keys,
                                                    int **secondary_key_lens,
                                                    int *num_keys, int info_type);
void mdhimReleaseSecondaryBulkInfo(secondary_bulk_info_t *si);

int mdhimWhichServer(mdhim_t *md, void *key, int key_len);

#ifdef __cplusplus
}
#endif
#endif
