#ifndef HXHIM_DOUBLE_FUNCTIONS_HPP
#define HXHIM_DOUBLE_FUNCTIONS_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "hxhim/double.h"
#include "hxhim/struct.h"

namespace hxhim {

int PutDouble(hxhim_t *hx,
              void *subject, std::size_t subject_len,
              void *predicate, std::size_t predicate_len,
              double *object);

int GetDouble(hxhim_t *hx,
              void *subject, std::size_t subject_len,
              void *predicate, std::size_t predicate_len);

int GetOpDouble(hxhim_t *hx,
                void *subject, std::size_t subject_len,
                void *predicate, std::size_t predicate_len,
                std::size_t num_records, enum hxhim_get_op_t op);

int BPutDouble(hxhim_t *hx,
               void **subjects, std::size_t *subject_lens,
               void **predicates, std::size_t *predicate_lens,
               double **objects,
               const std::size_t count);

int BGetDouble(hxhim_t *hx,
               void **subjects, std::size_t *subject_lens,
               void **predicates, std::size_t *predicate_lens,
               const std::size_t count);

int BGetOpDouble(hxhim_t *hx,
                 void **subjects, std::size_t *subject_lens,
                 void **predicates, std::size_t *predicate_lens,
                 std::size_t *num_records, enum hxhim_get_op_t *ops,
                 const std::size_t count);

}

#endif
