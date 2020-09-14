#ifndef HXHIM_FLOAT_FUNCTIONS_HPP
#define HXHIM_FLOAT_FUNCTIONS_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "hxhim/float.h"
#include "hxhim/struct.h"

namespace hxhim {

int PutFloat(hxhim_t *hx,
             void *subject, std::size_t subject_len,
             void *predicate, std::size_t predicate_len,
             float *object);

int GetFloat(hxhim_t *hx,
             void *subject, std::size_t subject_len,
             void *predicate, std::size_t predicate_len);

int GetOpFloat(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len,
               std::size_t num_records, enum hxhim_getop_t op);

int BPutFloat(hxhim_t *hx,
              void **subjects, std::size_t *subject_lens,
              void **predicates, std::size_t *predicate_lens,
              float **objects,
              const std::size_t count);

int BGetFloat(hxhim_t *hx,
              void **subjects, std::size_t *subject_lens,
              void **predicates, std::size_t *predicate_lens,
              const std::size_t count);

int BGetOpFloat(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                std::size_t *num_records, enum hxhim_getop_t *ops,
                const std::size_t count);
}

#endif
