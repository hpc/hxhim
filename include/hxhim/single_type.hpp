#ifndef HXHIM_BULK_DATA_SINGLE_TYPE_HPP
#define HXHIM_BULK_DATA_SINGLE_TYPE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "hxhim/struct.h"

namespace hxhim {

int BPutSingleType(hxhim_t *hx,
                   void **subjects, std::size_t *subject_lens,
                   void **predicates, std::size_t *predicate_lens,
                   enum hxhim_object_type_t object_type, void **objects, std::size_t *object_lens,
                   std::size_t count);

int BGetSingleType(hxhim_t *hx,
                   void **subjects, std::size_t *subject_lens,
                   void **predicates, std::size_t *predicate_lens,
                   enum hxhim_object_type_t object_type,
                   std::size_t count);

}

#endif
