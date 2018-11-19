#include "hxhim/private.hpp"
#include "hxhim/shuffle.hpp"
#include "utils/macros.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace hxhim {
namespace shuffle {

/**
 * Put
 * Places a set of Put data into the correct buffer for sending to a backend
 */
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
        std::map<std::pair<void *, void *>, int> &hashed) {

    // destination datastore
    int ds_id = -1;

    // find the destination datastore
    const std::pair<void *, void *> sp = std::make_pair(subject, predicate);
    std::map<std::pair<void *, void *>, int>::const_iterator h = hashed.find(sp);
    // not found, so hash
    if (h == hashed.end()) {
        ds_id = hx->p->hash.func(hx, subject, subject_len, predicate, predicate_len, hx->p->hash.args);
    }
    // use found value
    else {
        ds_id = h->second;
    }

    // get the destination backend id for the key
    if (ds_id < 0) {
        mlog(HXHIM_CLIENT_WARN, "Hash returned bad target datastore: %d", ds_id);
        return -1;
    }

    mlog(HXHIM_CLIENT_DBG, "Shuffle Put into datastore %d", ds_id);

    // split the backend id into destination rank and ds_offset
    const int ds_rank = hxhim::datastore::get_rank(hx, ds_id);
    const int ds_offset = hxhim::datastore::get_offset(hx, ds_id);

    // group local keys
    if (ds_rank == hx->p->bootstrap.rank) {
        std::size_t &i = local->count;

        // only add the key if there is space
        if (i < max_per_dst) {
            mlog(HXHIM_CLIENT_DBG, "Shuffle Local Put into index %zu", i);
            local->ds_offsets[i] = ds_offset;
            local->subjects[i] = subject;
            local->subject_lens[i] = subject_len;
            local->predicates[i] = predicate;
            local->predicate_lens[i] = predicate_len;
            local->object_types[i] = object_type;
            local->objects[i] = object;
            local->object_lens[i] = object_len;
            i++;
        }
        // else, return a bad destination datastore
        else {
            mlog(HXHIM_CLIENT_DBG, "Shuffle Local Put Could not add to packet");
            if (h == hashed.end()) {
                hashed[sp] = ds_id;
            }
            return -1;
        }
    }
    // group remote keys
    else {
        // try to find the destination first
        REF(remote)::iterator it = remote.find(ds_rank);

        // if the destination is not found, check if there is space for another one
        if (it == remote.end()) {
            mlog(HXHIM_CLIENT_DBG, "Shuffle Remote Put could not find destination rank %d", ds_rank);

            // if there is space for a new destination, insert it
            if ((remote.size() < max_remote) && hx->p->memory_pools.requests->unused()) {
                mlog(HXHIM_CLIENT_DBG, "Shuffle Remote Put adding destination rank %d", ds_rank);

                it = remote.insert(std::make_pair(ds_rank, hx->p->memory_pools.requests->acquire<Transport::Request::BPut>(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, max_per_dst))).first;
                it->second->src = hx->p->bootstrap.rank;
                it->second->dst = ds_rank;
            }
            else {
                mlog(HXHIM_CLIENT_DBG, "Shuffle Remote Put could not add destination %d %zu %zu", ds_id, remote.size(), max_remote);
                return -1;
            }
        }

        Transport::Request::BPut *rem = it->second;
        std::size_t &i = rem->count;

        // if there is space in the request packet, insert
        if (i < max_per_dst) {
            mlog(HXHIM_CLIENT_DBG, "Shuffle Remote Put into index %zu of rank %d", i, ds_rank);
            rem->ds_offsets[i] = ds_offset;
            rem->subjects[i] = subject;
            rem->subject_lens[i] = subject_len;
            rem->predicates[i] = predicate;
            rem->predicate_lens[i] = predicate_len;
            rem->object_types[i] = object_type;
            rem->objects[i] = object;
            rem->object_lens[i] = object_len;
            i++;
        }
        else {
            mlog(HXHIM_CLIENT_DBG, "Shuffle Remote Put Could not add to packet");
            if (h == hashed.end()) {
                hashed[sp] = ds_id;
            }
            return -1;
        }
    }

    return ds_id;
}

/**
 * Get
 * Places a set of Get data into the correct buffer for sending to a backend
 */
int Get(hxhim_t *hx,
        const std::size_t max_per_dst,
        void *subject,
        std::size_t subject_len,
        void *predicate,
        std::size_t predicate_len,
        hxhim_type_t object_type,
        Transport::Request::BGet *local,
        std::unordered_map<int, Transport::Request::BGet *> &remote,
        const std::size_t max_remote) {
    // get the destination backend id for the key
    const int ds_id = hx->p->hash.func(hx, subject, subject_len, predicate, predicate_len, hx->p->hash.args);
    if (ds_id < 0) {
        mlog(HXHIM_CLIENT_WARN, "Hash returned bad target datastore: %d", ds_id);
        return -1;
    }

    mlog(HXHIM_CLIENT_DBG, "Shuffle Get from datastore %d", ds_id);

    // split the backend id into destination rank and ds_offset
    const int ds_rank = hxhim::datastore::get_rank(hx, ds_id);
    const int ds_offset = hxhim::datastore::get_offset(hx, ds_id);

    // group local keys
    if (ds_rank == hx->p->bootstrap.rank) {
        // only add the key if there is space
        if (local->count < max_per_dst) {
            local->ds_offsets[local->count] = ds_offset;
            local->subjects[local->count] = subject;
            local->subject_lens[local->count] = subject_len;
            local->predicates[local->count] = predicate;
            local->predicate_lens[local->count] = predicate_len;
            local->object_types[local->count] = object_type;
            local->count++;
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
                it = remote.insert(std::make_pair(ds_rank, hx->p->memory_pools.requests->acquire<Transport::Request::BGet>(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, max_per_dst))).first;
                it->second->src = hx->p->bootstrap.rank;
                it->second->dst = ds_rank;
            }
            else {
                return -1;
            }
        }

        Transport::Request::BGet *rem = it->second;

        // if there is space in the request packet, insert
        if (rem->count < max_per_dst) {
            rem->ds_offsets[rem->count] = ds_offset;
            rem->subjects[rem->count] = subject;
            rem->subject_lens[rem->count] = subject_len;
            rem->predicates[rem->count] = predicate;
            rem->predicate_lens[rem->count] = predicate_len;
            rem->object_types[rem->count] = object_type;
            rem->count++;
        }
        else {
            return -1;
        }
    }

    return ds_id;
}

/**
 * GetOp
 * Places a set of GetOp data into the correct buffer for sending to a backend
 */
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
          const std::size_t max_remote) {
    // get the destination backend id for the key
    const int ds_id = hx->p->hash.func(hx, subject, subject_len, predicate, predicate_len, hx->p->hash.args);
    if (ds_id < 0) {
        mlog(HXHIM_CLIENT_WARN, "Hash returned bad target datastore: %d", ds_id);
        return -1;
    }

    mlog(HXHIM_CLIENT_DBG, "Shuffle GetOp from datastore %d", ds_id);

    // split the backend id into destination rank and ds_offset
    const int ds_rank = hxhim::datastore::get_rank(hx, ds_id);
    const int ds_offset = hxhim::datastore::get_offset(hx, ds_id);

    // group local keys
    if (ds_rank == hx->p->bootstrap.rank) {
        // only add the key if there is space
        if (local->count < max_per_dst) {
            local->ds_offsets[local->count] = ds_offset;
            local->subjects[local->count] = subject;
            local->subject_lens[local->count] = subject_len;
            local->predicates[local->count] = predicate;
            local->predicate_lens[local->count] = predicate_len;
            local->object_types[local->count] = object_type;
            local->num_recs[local->count] = recs;
            local->ops[local->count] = op;
            local->count++;
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
                it = remote.insert(std::make_pair(ds_rank, hx->p->memory_pools.requests->acquire<Transport::Request::BGetOp>(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, max_per_dst))).first;
                it->second->src = hx->p->bootstrap.rank;
                it->second->dst = ds_rank;
            }
            else {
                return -1;
            }
        }

        Transport::Request::BGetOp *rem = it->second;

        // if there is space in the request packet, insert
        if (rem->count < max_per_dst) {
            rem->ds_offsets[rem->count] = ds_offset;
            rem->subjects[rem->count] = subject;
            rem->subject_lens[rem->count] = subject_len;
            rem->predicates[rem->count] = predicate;
            rem->predicate_lens[rem->count] = predicate_len;
            rem->object_types[rem->count] = object_type;
            rem->num_recs[rem->count] = recs;
            rem->ops[rem->count] = op;
            rem->count++;
        }
        else {
            return -1;
        }
    }

    return ds_id;
}

/**
 * Delete
 * Places a set of Delete data into the correct buffer for sending to a backend
 */
int Delete(hxhim_t *hx,
           const std::size_t max_per_dst,
           void *subject,
           std::size_t subject_len,
           void *predicate,
           std::size_t predicate_len,
           Transport::Request::BDelete *local,
           std::unordered_map<int, Transport::Request::BDelete *> &remote,
           const std::size_t max_remote) {
    // get the destination backend id for the key
    const int ds_id = hx->p->hash.func(hx, subject, subject_len, predicate, predicate_len, hx->p->hash.args);
    if (ds_id < 0) {
        mlog(HXHIM_CLIENT_WARN, "Hash returned bad target datastore: %d", ds_id);
        return -1;
    }

    mlog(HXHIM_CLIENT_DBG, "Shuffle Delete from datastore %d", ds_id);

    // split the backend id into destination rank and ds_offset
    const int ds_rank = hxhim::datastore::get_rank(hx, ds_id);
    const int ds_offset = hxhim::datastore::get_offset(hx, ds_id);

    // group local keys
    if (ds_rank == hx->p->bootstrap.rank) {
        // only add the key if there is space
        if (local->count < max_per_dst) {
            local->ds_offsets[local->count] = ds_offset;
            local->subjects[local->count] = subject;
            local->subject_lens[local->count] = subject_len;
            local->predicates[local->count] = predicate;
            local->predicate_lens[local->count] = predicate_len;
            local->count++;
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
                it = remote.insert(std::make_pair(ds_rank, hx->p->memory_pools.requests->acquire<Transport::Request::BDelete>(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, max_per_dst))).first;
                it->second->src = hx->p->bootstrap.rank;
                it->second->dst = ds_rank;
            }
            else {
                return -1;
            }
        }

        Transport::Request::BDelete *rem = it->second;

        // if there is space in the request packet, insert
        if (rem->count < max_per_dst) {
            rem->ds_offsets[rem->count] = ds_offset;
            rem->subjects[rem->count] = subject;
            rem->subject_lens[rem->count] = subject_len;
            rem->predicates[rem->count] = predicate;
            rem->predicate_lens[rem->count] = predicate_len;
            rem->count++;
        }
        else {
            return -1;
        }
    }

    return ds_id;
}

/**
 * Histogram
 * Places a Histogram request into the correct buffer for sending to a backend
 */
int Histogram(hxhim_t *hx,
              const std::size_t max_per_dst,
              const int ds_id,
              Transport::Request::BHistogram *local,
              std::unordered_map<int, Transport::Request::BHistogram *> &remote,
              const std::size_t max_remote) {
    if (ds_id < 0) {
        mlog(HXHIM_CLIENT_WARN, "Bad Histogram source: %d", ds_id);
        return -1;
    }

    mlog(HXHIM_CLIENT_DBG, "Shuffle Histogram from datastore %d", ds_id);

    // split the backend id into destination rank and ds_offset
    const int ds_rank = hxhim::datastore::get_rank(hx, ds_id);
    const int ds_offset = hxhim::datastore::get_offset(hx, ds_id);

    // group local keys
    if (ds_rank == hx->p->bootstrap.rank) {
        // only add the destination if there is space
        if (local->count < max_per_dst) {
            local->ds_offsets[local->count] = ds_offset;
            local->count++;
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
                it = remote.insert(std::make_pair(ds_rank, hx->p->memory_pools.requests->acquire<Transport::Request::BHistogram>(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, max_per_dst))).first;
                it->second->src = hx->p->bootstrap.rank;
                it->second->dst = ds_rank;
            }
            else {
                return -1;
            }
        }

        Transport::Request::BHistogram *rem = it->second;

        if (rem->count < max_per_dst) {
            rem->ds_offsets[rem->count] = ds_offset;
            rem->count++;
        }
        else {
            return -1;
        }
    }

    return ds_id;
}

}
}
