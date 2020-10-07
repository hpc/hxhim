#ifndef HXHIM_SHUFFLE_HPP
#define HXHIM_SHUFFLE_HPP

#include <cstddef>
#include <unordered_map>

#include "hxhim/constants.h"
#include "transport/Messages/Messages.hpp"
#include "utils/Stats.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace hxhim {
namespace shuffle {

const int NOSPACE = -1;

/**
 * shuffle
 * Place user data into a packet for sending to a datastore.
 *
 * 1. If the destination is not known, calculate it
 *    (allows for calculations to be skipped)
 * 2. Find an existing destination packet. If one
 *    is not found, create it.
 * 3. If the destination buffer cannot accept any
 *    more data, return NOSPACE.
 * 3. Place the data into the destination buffer.
 *
 *
 *@tparam cache_t          the cache type of the request
 *@tparam Request_t        the response packet type
 *@param src               a single request that needs to be shuffled
 *@param max_ops_per_send  the maximum number of ops that can be placed into a request packet
 *@param remote            the bulk op packets destined for remote range servers
 *@return                  destination datastore on success, or NOSPACE
 */
template <typename cache_t, typename Request_t>
int shuffle(cache_t *src,
            const std::size_t max_ops_per_send,
            Request_t **remote) {
    #if PRINT_FIND_DST
    ::Stats::Chronostamp find_dst;
    find_dst.start = ::Stats::now();
    #endif

    mlog(HXHIM_CLIENT_INFO, "Shuffled %p to datastore %d on rank %d", src, src->ds_id, src->ds_rank);

    Request_t *&dst = remote[src->ds_rank];
    if (!dst) {
        dst = construct<Request_t>(max_ops_per_send);
    }

    mlog(HXHIM_CLIENT_INFO, "Packet going to rank %d has %zu already packed out of %zu slots", src->ds_rank, dst->count, max_ops_per_send);

    #if PRINT_FIND_DST
    find_dst.end = ::Stats::now();

    // place this time range into src because the target bulk message might not have space
    src->timestamps->find_dsts.emplace_back(find_dst);
    #endif

    // packet is full
    // don't set bulk timestamps here since
    // the src will eventually be bulked
    if (dst->count >= max_ops_per_send) {
        mlog(HXHIM_CLIENT_INFO, "Cannot add to packet going to rank %d (no space)", src->ds_rank);
        return NOSPACE;
    }

    ::Stats::Chronostamp bulked;
    bulked.start = ::Stats::now();
    src->moveto(dst);
    bulked.end = ::Stats::now();

    // set timestamp here because src gets moved into dst
    dst->timestamps.reqs[dst->count - 1]->bulked = bulked;

    mlog(HXHIM_CLIENT_INFO, "Added %p to rank %d packet (%zu / %zu)", src, src->ds_id, dst->count, max_ops_per_send);

    return src->ds_id;
}

}
}

#endif
