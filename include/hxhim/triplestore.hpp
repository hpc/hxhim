#ifndef HXHIM_TRIPLESTORE_HPP
#define HXHIM_TRIPLESTORE_HPP

#include "hxhim/Blob.hpp"

/** @description Combines a subject and predicate into a key */
int sp_to_key(const Blob &subject,
              const Blob &predicate,
              std::string &key);

/** @description Splits a key into a subject and predicate */
int key_to_sp(const void *key,
              const std::size_t key_len,
              Blob &subject,
              Blob &predicate,
              const bool copy);

int key_to_sp(const std::string &key,
              Blob &subject,
              Blob &predicate,
              const bool copy);
#endif
