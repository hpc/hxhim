#ifndef HXHIM_SHUFFLE_HPP
#define HXHIM_SHUFFLE_HPP

#include <cstddef>
#include <map>
#include <unordered_map>

#include "hxhim/constants.h"
#include "hxhim/struct.h"
#include "transport/Messages/Messages.hpp"

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
 * The remote buffer should be a map of destination rank to bulk op, with all data in each bulk op going to one rank (but possibly multiple backends)
 *
 */

namespace shuffle {

int Put(hxhim_t *hx,
        const std::size_t max_per_dst,
        void *subject,
        std::size_t subject_len,
        void *predicate,
        std::size_t predicate_len,
        hxhim_type_t object_type,
        void *object,
        std::size_t object_len,
        Transport::Request::BPut *local,
        std::unordered_map<int, Transport::Request::BPut *> &remote,
        const std::size_t max_remote,
        std::map<std::pair<void *, void *>, int> &hashed);

int Get(hxhim_t *hx,
        const std::size_t max_per_dst,
        void *subject,
        std::size_t subject_len,
        void *predicate,
        std::size_t predicate_len,
        hxhim_type_t object_type,
        Transport::Request::BGet *local,
        std::unordered_map<int, Transport::Request::BGet *> &remote,
        const std::size_t max_remote);

int GetOp(hxhim_t *hx,
          const std::size_t max_per_dst,
          void *subject,
          std::size_t subject_len,
          void *predicate,
          std::size_t predicate_len,
          hxhim_type_t object_type,
          const std::size_t recs, const hxhim_get_op_t op,
          Transport::Request::BGetOp *local,
          std::unordered_map<int, Transport::Request::BGetOp *> &remote,
          const std::size_t max_remote);

int Delete(hxhim_t *hx,
           const std::size_t max_per_dst,
           void *subject,
           std::size_t subject_len,
           void *predicate,
           std::size_t predicate_len,
           Transport::Request::BDelete *local,
           std::unordered_map<int, Transport::Request::BDelete *> &remote,
           const std::size_t max_remote);

int Histogram(hxhim_t *hx,
              const std::size_t max_per_dst,
              const int ds_id,
              Transport::Request::BHistogram *local,
              std::unordered_map<int, Transport::Request::BHistogram *> &remote,
              const std::size_t max_remote);

}
}

#endif
