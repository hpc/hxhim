#ifndef HXHIM_DOUBLE_FUNCTIONS_HPP
#define HXHIM_DOUBLE_FUNCTIONS_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "hxhim/double.h"
#include "hxhim/struct.h"

namespace hxhim {

int PutDouble(hxhim_t *hx,
              void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
              void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type,
              double *object,
              const hxhim_put_permutation_t permutations);

int GetDouble(hxhim_t *hx,
              void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
              void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type);

int GetOpDouble(hxhim_t *hx,
                void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
                void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type,
                std::size_t num_records, enum hxhim_getop_t op);

int BPutDouble(hxhim_t *hx,
               void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
               void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
               double **objects,
               const hxhim_put_permutation_t *permutations,
               const std::size_t count);

int BGetDouble(hxhim_t *hx,
               void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
               void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
               const std::size_t count);

int BGetOpDouble(hxhim_t *hx,
                 void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
                 void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                 std::size_t *num_records, enum hxhim_getop_t *ops,
                 const std::size_t count);
}

#endif
