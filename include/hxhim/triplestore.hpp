#ifndef HXHIM_TRIPLESTORE_HPP
#define HXHIM_TRIPLESTORE_HPP

#include "utils/Blob.hpp"

/** @description Combines a subject and predicate into a key */
int sp_to_key(const Blob &subject,
              const Blob &predicate,
              Blob *key);

/** @description Splits a key into a subject and predicate */
int key_to_sp(const Blob &key,
              Blob &subject,
              Blob &predicate,
              const bool copy);

#endif
