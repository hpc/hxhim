#ifndef HXHIM_TRIPLESTORE
#define HXHIM_TRIPLESTORE

#include <type_traits>

#include "constants.h"

/** @description Combines a subject and predicate into a key */
int sp_to_key(const void *subject, const std::size_t subject_len,
              const void *predicate, const std::size_t predicate_len,
              void **key, std::size_t *key_len);

/** @description Splits a key into a subject and predicate */
int key_to_sp(const void *key, const std::size_t key_len,
              void **subject, std::size_t *subject_len,
              void **predicate, std::size_t *predicate_len);
#endif
