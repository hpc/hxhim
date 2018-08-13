#ifndef HXHIM_HASH_H
#define HXHIM_HASH_H

#include <stdint.h>

#include "hxhim/struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * hash function signature typedef that maps subject-predicate pairs to produce datastore IDs
 *
 * The args argument is somewhat redundant, as it can be accessed through hx->p->hash_args,
 * but is provided in order to allow for including "hxhim/private.hpp" to be optional.
 *
 * @param hx the HXHIM state
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param args           extra arguments needed by the function
 * @return the datastore ID of where the subject-predicate key should go
 */
typedef int (*hxhim_hash_t)(hxhim_t *hx, void *subject, const size_t subject_len, void *predicate, const size_t predicate_len, void *args);

#ifdef __cplusplus
}
#endif

#endif
