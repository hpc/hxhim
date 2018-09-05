#ifndef HXHIM_HASH_HPP
#define HXHIM_HASH_HPP

#include <cstddef>

#include "hxhim/struct.h"
#include "hxhim/hash.h"

namespace hxhim {
namespace hash {

int Rank(hxhim_t *hx, void *subject, const std::size_t subject_len, void *predicate, const std::size_t predicate_len, void *args);
int SumModDatastores(hxhim_t *hx, void *subject, const std::size_t subject_len, void *predicate, const std::size_t predicate_len, void *args);
int Local(hxhim_t *hx, void *subject, const std::size_t subject_len, void *predicate, const std::size_t predicate_len, void *args);

}
}

#endif
