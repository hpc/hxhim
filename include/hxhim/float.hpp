#ifndef HXHIM_FLOAT_FUNCTIONS_HPP
#define HXHIM_FLOAT_FUNCTIONS_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "hxhim/float.h"
#include "hxhim/struct.h"

namespace hxhim {

int PutFloat(hxhim_t *hx,
             void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
             void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type,
             float *object,
             const hxhim_put_permutation_t permutations);

int GetFloat(hxhim_t *hx,
             void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
             void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type);

int GetOpFloat(hxhim_t *hx,
               void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
               void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type,
               std::size_t num_records, enum hxhim_getop_t op);

int BPutFloat(hxhim_t *hx,
              void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
              void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
              float **objects,
              const hxhim_put_permutation_t *permutations,
              const std::size_t count);

int BGetFloat(hxhim_t *hx,
              void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
              void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
              const std::size_t count);

int BGetOpFloat(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
                void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                std::size_t *num_records, enum hxhim_getop_t *ops,
                const std::size_t count);
}

#endif
