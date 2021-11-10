#ifndef HXHIM_HASH_H
#define HXHIM_HASH_H

#include "hxhim/struct.h"

#ifdef __cplusplus
#include <cstddef>
extern "C"
{
#else
#include <stddef.h>
#endif

/**
 * hash function signature typedef that maps subject-predicate pairs to datastore IDs
 *
 * The args argument is somewhat redundant, as it can be accessed through hx->p->hash_args,
 * but is provided in order to allow for including "hxhim/private/hxhim.hpp" to not be necessary.
 * If private values are needed for hash functions defined outside of "hxhim/hashes.cpp",
 * some accessor functions are provided in "hxhim/accessors.{h,hpp}".
 *
 * @param hx the HXHIM state
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the predicate
 * @param args           extra arguments needed by the function
 * @return the datastore ID of where the subject-predicate pair should go
 */
typedef int (*hxhim_hash_t)(hxhim_t *hx,
                            void *subject, const size_t subject_len,
                            void *predicate, const size_t predicate_len,
                            void *args);

/** This function wraps the chosen hash function and performs checks */
int hxhim_hash(hxhim_t *hx,
               void *subject, const size_t subject_len,
               void *predicate, const size_t predicate_len);

/** @description Simple, predefined hashes */
int hxhim_hash_DatastoreZero(hxhim_t *hx,
                             void *subject, const size_t subject_len,
                             void *predicate, const size_t predicate_len,
                             void *args);

int hxhim_hash_RankModDatastores(hxhim_t *hx,
                                 void *subject, const size_t subject_len,
                                 void *predicate, const size_t predicate_len,
                                 void *args);

int hxhim_hash_SumModDatastores(hxhim_t *hx,
                                void *subject, const size_t subject_len,
                                void *predicate, const size_t predicate_len,
                                void *args);

int hxhim_hash_uthash_BER(hxhim_t *hx,
                          void *subject, const size_t subject_len,
                          void *predicate, const size_t predicate_len,
                          void *);

int hxhim_hash_uthash_SAX(hxhim_t *hx,
                          void *subject, const size_t subject_len,
                          void *predicate, const size_t predicate_len,
                          void *);

int hxhim_hash_uthash_FNV(hxhim_t *hx,
                          void *subject, const size_t subject_len,
                          void *predicate, const size_t predicate_len,
                          void *);

int hxhim_hash_uthash_OAT(hxhim_t *hx,
                          void *subject, const size_t subject_len,
                          void *predicate, const size_t predicate_len,
                          void *);

int hxhim_hash_uthash_JEN(hxhim_t *hx,
                          void *subject, const size_t subject_len,
                          void *predicate, const size_t predicate_len,
                          void *);

int hxhim_hash_uthash_SFH(hxhim_t *hx,
                          void *subject, const size_t subject_len,
                          void *predicate, const size_t predicate_len,
                          void *);

#ifdef __cplusplus
}
#endif

#endif
