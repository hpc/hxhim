#ifndef HXHIM_SHUFFLE_HPP
#define HXHIM_SHUFFLE_HPP

#include <cstddef>
#include <unordered_map>

#include "hxhim/accessors.hpp"
#include "hxhim/constants.h"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/struct.h"
#include "transport/Messages/Messages.hpp"
#include "utils/macros.hpp"
#include "utils/memory.hpp"

namespace hxhim {
namespace shuffle {

const int ERROR = -2;
const int NOSPACE = -1;

/**
 * shuffle
 * Functions in this namespace take in 1 entry that is to be
 * sent to be processed by the backend and does 2 things:
 *
 *     1. Figures out which backend the data should go to
 *     2. Packs the data into either the local or remote buffer
 *
 * The local buffer is a single bulk op, with all data going to one rank (but possibly multiple backends)
 * The remote buffer should be a map of destination rank to bulk op, with all data in each bulk op going to one rank (but possibly multiple backends)
 *
 *
 *@tparam SRC_t       the cache type of the request
 *@tparam DST_t       the response packet type
 *@param hx           the HXHIM instance
 *@param rank         the rank this HXHIM process is on
 *@param max_per_dst  the maximum number of requests allowed in a bulk packet
 *@param src          a single request that needs to be shuffled
 *@param local        the bulk op packet destined for the local range server
 *@param remote       the bulk op packets destined for remote range servers
 *@return             destination datastore on success, or ERROR or NOSPACE
 */
template <typename SRC_t, typename DST_t>
int shuffle(hxhim_t *hx,
            const int rank,
            const std::size_t max_per_dst,
            SRC_t *src,
            DST_t *local,
            std::unordered_map<int, DST_t *> &remote) {
    // skip duplicate calculations
    if ((src->ds_id < 0) || (src->ds_rank < 0) || (src->ds_offset < 0)) {
        clock_gettime(CLOCK_MONOTONIC, &src->first_shuffle);

        // get the destination backend id for the key
        clock_gettime(CLOCK_MONOTONIC, &src->hash.start);
        src->ds_id = hx->p->hash.func(hx,
                                      src->subject->data(), src->subject->size(),
                                      src->predicate->data(), src->predicate->size(),
                                      hx->p->hash.args);
        clock_gettime(CLOCK_MONOTONIC, &src->hash.end);

        if (src->ds_id < 0) {
            mlog(HXHIM_CLIENT_WARN, "Hash returned bad target datastore: %d", src->ds_id);
            return ERROR;
        }

        // split the backend id into destination rank and ds_offset
        src->ds_rank = hxhim::datastore::get_rank(hx, src->ds_id);
        src->ds_offset = hxhim::datastore::get_offset(hx, src->ds_id);
    }

    mlog(HXHIM_CLIENT_INFO, "Shuffled %p to datastore %d on rank %d", src, src->ds_id, src->ds_rank);

    DST_t *dst = nullptr;

    // local keys
    if (src->ds_rank == rank) {
        dst = local;
    }
    // remote keys
    else {
        // try to find the destination first
        REF(remote)::iterator it = remote.find(src->ds_rank);

        // if the destination is not found, add one
        if (it == remote.end()) {
            mlog(HXHIM_CLIENT_DBG, "Creating new packet going to rank %d", src->ds_rank);

            it = remote.insert(std::make_pair(src->ds_rank, construct<DST_t>(max_per_dst))).first;
            it->second->src = rank;
            it->second->dst = src->ds_rank;
        }

        dst = it->second;
    }

    mlog(HXHIM_CLIENT_INFO, "Packet going to rank %d has %zu already packed out of %zu slots", src->ds_rank, dst->count, max_per_dst);

    // packet is full
    if (dst->count >= max_per_dst) {
        mlog(HXHIM_CLIENT_INFO, "Cannot add to packet going to rank %d (no space)", src->ds_rank);
        return NOSPACE;
    }

    src->moveto(dst);
    mlog(HXHIM_CLIENT_INFO, "Added %p to rank %d packet (%zu / %zu)", src, src->ds_id, dst->count, max_per_dst);

    dst->timestamps.reqs[dst->count - 1].cached = src->added;
    dst->timestamps.reqs[dst->count - 1].shuffled = src->first_shuffle;
    clock_gettime(CLOCK_MONOTONIC, &dst->timestamps.reqs[dst->count - 1].bulked);

    return src->ds_id;
}

}
}

#endif
