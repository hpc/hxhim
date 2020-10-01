#ifndef HXHIM_SHUFFLE_HPP
#define HXHIM_SHUFFLE_HPP

#include <cstddef>
#include <unordered_map>

#include "hxhim/constants.h"
#include "hxhim/private/hxhim.hpp"
#include "transport/Messages/Messages.hpp"
#include "utils/Stats.hpp"
#include "utils/macros.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace hxhim {
namespace shuffle {

const int ERROR = -2;
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
 *@tparam cache_t     the cache type of the request
 *@tparam Request_t   the response packet type
 *@param hx           the HXHIM instance
 *@param src          a single request that needs to be shuffled
 *@param remote       the bulk op packets destined for remote range servers
 *@return             destination datastore on success, or ERROR or NOSPACE
 */
template <typename cache_t, typename Request_t>
int shuffle(hxhim_t *hx,
            cache_t *src,
            Request_t **remote) {
    // skip duplicate calculations
    if ((src->ds_id < 0) || (src->ds_rank < 0)) {
        // this should be the first and only time hash is called on this packet
        src->timestamps->shuffled = ::Stats::now();

        // get the destination backend id for the key
        src->timestamps->hashed.start = ::Stats::now();
        src->ds_id = hx->p->hash.func(hx,
                                      src->subject.data(), src->subject.size(),
                                      src->predicate.data(), src->predicate.size(),
                                      hx->p->hash.args);
        src->timestamps->hashed.end = ::Stats::now();

        if (src->ds_id < 0) {
            mlog(HXHIM_CLIENT_WARN, "Hash returned bad target datastore: %d", src->ds_id);
            return ERROR;
        }

        // split the backend id into destination rank and ds_offset
        src->ds_rank = hxhim::datastore::get_rank(hx, src->ds_id);
    }

    ::Stats::Chronostamp find_dst;
    find_dst.start = ::Stats::now();

    mlog(HXHIM_CLIENT_INFO, "Shuffled %p to datastore %d on rank %d", src, src->ds_id, src->ds_rank);

    Request_t *&dst = remote[src->ds_rank];
    if (!dst) {
        dst = construct<Request_t>(hx->p->max_ops_per_send);
        dst->src = hx->p->bootstrap.rank;
        dst->dst = src->ds_rank;
    }

    mlog(HXHIM_CLIENT_INFO, "Packet going to rank %d has %zu already packed out of %zu slots", src->ds_rank, dst->count, hx->p->max_ops_per_send);

    find_dst.end = ::Stats::now();

    // place this time range into src because the target bulk message might not have space
    src->timestamps->find_dsts.emplace_back(find_dst);

    // packet is full
    if (dst->count >= hx->p->max_ops_per_send) {
        mlog(HXHIM_CLIENT_INFO, "Cannot add to packet going to rank %d (no space)", src->ds_rank);
        return NOSPACE;
    }

    ::Stats::Chronostamp bulked;
    bulked.start = ::Stats::now();
    src->moveto(dst);
    bulked.end = ::Stats::now();

    // set timestamp here because src gets moved into dst
    dst->timestamps.reqs[dst->count - 1]->bulked = bulked;

    mlog(HXHIM_CLIENT_INFO, "Added %p to rank %d packet (%zu / %zu)", src, src->ds_id, dst->count, hx->p->max_ops_per_send);

    return src->ds_id;
}

}
}

#endif
