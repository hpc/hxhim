#ifndef HXHIM_SHUFFLE_HPP
#define HXHIM_SHUFFLE_HPP

#include <cstddef>
#include <unordered_map>

#include "hxhim/constants.h"
#include "hxhim/private.hpp"
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
 *
 * The local buffer is a single bulk op, with all data going to one rank (but possibly multiple backends)
 * The remote buffer should be a map of destination rank to bulk op, with all data in each bulk op going to one rank (but possibly multiple backends)
 *
 */
template <typename SRC_t, typename DST_t>
int shuffle(hxhim_t *hx,
            const std::size_t max_per_dst,
            SRC_t *src,
            DST_t *local,
            std::unordered_map<int, DST_t *> &remote,
            const std::size_t max_remote) {
    // get the destination backend id for the key
    const int ds_id = hx->p->hash.func(hx, src->subject, src->subject_len, src->predicate, src->predicate_len, hx->p->hash.args);
    if (ds_id < 0) {
        mlog(HXHIM_CLIENT_WARN, "Hash returned bad target datastore: %d", ds_id);
        return -1;
    }

    mlog(HXHIM_CLIENT_DBG, "Shuffle got datastore %d", ds_id);

    // split the backend id into destination rank and ds_offset
    const int ds_rank = hxhim::datastore::get_rank(hx, ds_id);
    const int ds_offset = hxhim::datastore::get_offset(hx, ds_id);

    // group local keys
    if (ds_rank == hx->p->bootstrap.rank) {
        // only add the key if there is space
        if (local->count < max_per_dst) {
            src->moveto(local, ds_offset);
        }
        // else, return a bad destination datastore
        else {
            return -1;
        }
    }
    // group remote keys
    else {
        // try to find the destination first
        REF(remote)::iterator it = remote.find(ds_rank);

        // if the destination is not found, check if there is space for another one
        if (it == remote.end()) {
            // if there is space for a new destination, insert it
            if (remote.size() < max_remote) {
                it = remote.insert(std::make_pair(ds_rank, hx->p->memory_pools.requests->acquire<DST_t>(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, max_per_dst))).first;
                it->second->src = hx->p->bootstrap.rank;
                it->second->dst = ds_rank;
            }
            else {
                mlog(HXHIM_CLIENT_DBG, "Shuffle Remote Put could not add destination %d %zu %zu", ds_id, remote.size(), max_remote);
                return -1;
            }
        }

        DST_t *rem = it->second;

        // if there is space in the request packet, insert
        if (rem->count < max_remote) {
            src->moveto(rem, ds_offset);
        }
        else {
            return -1;
        }
    }

    return ds_id;
}

}

#endif
