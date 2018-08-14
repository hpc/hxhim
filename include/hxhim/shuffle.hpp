#ifndef HXHIM_SHUFFLE_HPP
#define HXHIM_SHUFFLE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "hxhim/struct.h"
#include "transport/Messages.hpp"

namespace hxhim {

/**
 * shuffle
 * Functions in this namespace take in 1 entry that is to be
 * sent to be processed by the backend and does 2 things:
 *
 *     1. Figures out which backend the data should go to
 *     2. Packs the data into either the local or remote buffer
 *
 * All arguments passed into these functions should be defined.
 * The arguments are not checked in order to reduce branch predictions.
 *
 * The local buffer is a single bulk op, with all data going to one rank (but possibly multiple backends)
 * The remote buffer should be an array, with all data in each element going to one rank (but possibly multiple backends)
 *
 */

namespace shuffle {

int Put(hxhim_t *hx,
        const std::size_t max,
        void *subject,
        std::size_t subject_len,
        void *predicate,
        std::size_t predicate_len,
        hxhim_type_t object_type,
        void *object,
        std::size_t object_len,
        Transport::Request::BPut *local,
        Transport::Request::BPut **remote);

int Get(hxhim_t *hx,
        const std::size_t max,
        void *subject,
        std::size_t subject_len,
        void *predicate,
        std::size_t predicate_len,
        hxhim_type_t object_type,
        Transport::Request::BGet *local,
        Transport::Request::BGet **remote);

int GetOp(hxhim_t *hx,
          const std::size_t max,
          void *subject,
          std::size_t subject_len,
          void *predicate,
          std::size_t predicate_len,
          hxhim_type_t object_type,
          const std::size_t recs, const hxhim_get_op_t op,
          Transport::Request::BGetOp *local,
          Transport::Request::BGetOp **remote);

int Delete(hxhim_t *hx,
           const std::size_t max,
           void *subject,
           std::size_t subject_len,
           void *predicate,
           std::size_t predicate_len,
           Transport::Request::BDelete *local,
           Transport::Request::BDelete **remote);

}
}

#endif
