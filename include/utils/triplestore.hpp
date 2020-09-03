#ifndef HXHIM_TRIPLESTORE_HPP
#define HXHIM_TRIPLESTORE_HPP

#include "utils/Blob.hpp"

/** @description Combines a subject and predicate into a key */
char *sp_to_key(const Blob &subject,
                const Blob &predicate,
                char *&buf, std::size_t &buf_len,
                std::size_t &key_len);

/** @description Splits a key into a subject and predicate */
int key_to_sp(const void *key, const std::size_t key_len,
              Blob &subject,
              Blob &predicate,
              const bool copy);
#endif
