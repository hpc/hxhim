#ifndef HXHIM_BULK_DATA_SINGLE_TYPE_HPP
#define HXHIM_BULK_DATA_SINGLE_TYPE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "hxhim/single_type.h"
#include "hxhim/struct.h"

namespace hxhim {

int BPutSingleType(hxhim_t *hx,
                   void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
                   void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                   void **objects, std::size_t *object_lens, enum hxhim_data_t object_type,
                   const hxhim_put_permutation_t *permutations,
                   const std::size_t count);

int BGetSingleType(hxhim_t *hx,
                   void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
                   void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                   enum hxhim_data_t object_type,
                   const std::size_t count);

int BGetOpSingleType(hxhim_t *hx,
                     void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
                     void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                     enum hxhim_data_t object_type,
                     std::size_t *num_records, enum hxhim_getop_t *ops,
                     const std::size_t count);
}

#endif
