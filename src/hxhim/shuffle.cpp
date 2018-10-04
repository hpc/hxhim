#include "hxhim/private.hpp"
#include "hxhim/shuffle.hpp"
#include "utils/macros.hpp"

namespace hxhim {
namespace shuffle {

/**
 * Put
 * Places a set of Put data into the correct buffer for sending to a backend
 */
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
        std::map<int, Transport::Request::BPut *> &remote) {
    // get the destination backend id for the key
    const int ds_id = hx->p->hash.func(hx, subject, subject_len, predicate, predicate_len, hx->p->hash.args);

    if (ds_id > -1) {
        // split the backend id into destination rank and ds_offset
        const int dst = hxhim::datastore::get_rank(hx, ds_id);
        const int ds_offset = hxhim::datastore::get_offset(hx, ds_id);

        // group local keys
        if (dst == hx->p->bootstrap.rank) {
            // only add the key if there is space
            if (local->count < max) {
                local->ds_offsets[local->count] = ds_offset;
                local->subjects[local->count] = subject;
                local->subject_lens[local->count] = subject_len;
                local->predicates[local->count] = predicate;
                local->predicate_lens[local->count] = predicate_len;
                local->object_types[local->count] = object_type;
                local->objects[local->count] = object;
                local->object_lens[local->count] = object_len;
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
            REF(remote)::iterator it = remote.find(dst);

            // if the destination is not found, check if there is space for another one
            if (it == remote.end()) {
                // if there is space for a new destination, insert it
                if (remote.size() < (hx->p->memory_pools.requests->regions() / 2)) {
                    it = remote.insert(std::make_pair(dst, hx->p->memory_pools.requests->acquire<Transport::Request::BPut>(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, max))).first;
                    it->second->src = hx->p->bootstrap.rank;
                    it->second->dst = dst;
                }
                else {
                    return -1;
                }
            }

            Transport::Request::BPut *rem = it->second;

            // if there is space in the request packet, insert
            if (rem->count < max) {
                rem->ds_offsets[rem->count] = ds_offset;
                rem->subjects[rem->count] = subject;
                rem->subject_lens[rem->count] = subject_len;
                rem->predicates[rem->count] = predicate;
                rem->predicate_lens[rem->count] = predicate_len;
                rem->object_types[rem->count] = object_type;
                rem->objects[rem->count] = object;
                rem->object_lens[rem->count] = object_len;
                rem->count++;
            }
            else {
                return -1;
            }
        }
    }

    return ds_id;
}

/**
 * Get
 * Places a set of Get data into the correct buffer for sending to a backend
 */
int Get(hxhim_t *hx,
        const std::size_t max,
        void *subject,
        std::size_t subject_len,
        void *predicate,
        std::size_t predicate_len,
        hxhim_type_t object_type,
        Transport::Request::BGet *local,
        std::map<int, Transport::Request::BGet *> &remote) {
    // get the destination backend id for the key
    const int ds_id = hx->p->hash.func(hx, subject, subject_len, predicate, predicate_len, hx->p->hash.args);

    if (ds_id > -1) {
        // split the backend id into destination rank and ds_offset
        const int dst = hxhim::datastore::get_rank(hx, ds_id);
        const int ds_offset = hxhim::datastore::get_offset(hx, ds_id);

        // group local keys
        if (dst == hx->p->bootstrap.rank) {
            // only add the key if there is space
            if (local->count < max) {
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
            REF(remote)::iterator it = remote.find(dst);

            // if the destination is not found, check if there is space for another one
            if (it == remote.end()) {
                // if there is space for a new destination, insert it
                if (remote.size() < (hx->p->memory_pools.requests->regions() / 2)) {
                    it = remote.insert(std::make_pair(dst, hx->p->memory_pools.requests->acquire<Transport::Request::BGet>(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, max))).first;
                    it->second->src = hx->p->bootstrap.rank;
                    it->second->dst = dst;
                }
                else {
                    return -1;
                }
            }

            Transport::Request::BGet *rem = it->second;

            // if there is space in the request packet, insert
            if (rem->count < max) {
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
    }

    return ds_id;
}

/**
 * GetOp
 * Places a set of GetOp data into the correct buffer for sending to a backend
 */
int GetOp(hxhim_t *hx,
          const std::size_t max,
          void *subject,
          std::size_t subject_len,
          void *predicate,
          std::size_t predicate_len,
          hxhim_type_t object_type,
          const std::size_t recs, const hxhim_get_op_t op,
          Transport::Request::BGetOp *local,
          std::map<int, Transport::Request::BGetOp *> &remote) {
    // get the destination backend id for the key
    const int ds_id = hx->p->hash.func(hx, subject, subject_len, predicate, predicate_len, hx->p->hash.args);

    if (ds_id > -1) {
        // split the backend id into destination rank and ds_offset
        const int dst = hxhim::datastore::get_rank(hx, ds_id);
        const int ds_offset = hxhim::datastore::get_offset(hx, ds_id);

        // group local keys
        if (dst == hx->p->bootstrap.rank) {
            // only add the key if there is space
            if (local->count < max) {
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
            REF(remote)::iterator it = remote.find(dst);

            // if the destination is not found, check if there is space for another one
            if (it == remote.end()) {
                // if there is space for a new destination, insert it
                if (remote.size() < (hx->p->memory_pools.requests->regions() / 2)) {
                    it = remote.insert(std::make_pair(dst, hx->p->memory_pools.requests->acquire<Transport::Request::BGetOp>(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, max))).first;
                    it->second->src = hx->p->bootstrap.rank;
                    it->second->dst = dst;
                }
                else {
                    return -1;
                }
            }

            Transport::Request::BGetOp *rem = it->second;

            // if there is space in the request packet, insert
            if (rem->count < max) {
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
    }

    return ds_id;
}

/**
 * Delete
 * Places a set of Delete data into the correct buffer for sending to a backend
 */
int Delete(hxhim_t *hx,
           const std::size_t max,
           void *subject,
           std::size_t subject_len,
           void *predicate,
           std::size_t predicate_len,
           Transport::Request::BDelete *local,
           std::map<int, Transport::Request::BDelete *> &remote) {
    // get the destination backend id for the key
    const int ds_id = hx->p->hash.func(hx, subject, subject_len, predicate, predicate_len, hx->p->hash.args);

    if (ds_id > -1) {
        // split the backend id into destination rank and ds_offset
        const int dst = hxhim::datastore::get_rank(hx, ds_id);
        const int ds_offset = hxhim::datastore::get_offset(hx, ds_id);

        // group local keys
        if (dst == hx->p->bootstrap.rank) {
            // only add the key if there is space
            if (local->count < max) {
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
            REF(remote)::iterator it = remote.find(dst);

            // if the destination is not found, check if there is space for another one
            if (it == remote.end()) {
                // if there is space for a new destination, insert it
                if (remote.size() < (hx->p->memory_pools.requests->regions() / 2)) {
                    it = remote.insert(std::make_pair(dst, hx->p->memory_pools.requests->acquire<Transport::Request::BDelete>(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, max))).first;
                    it->second->src = hx->p->bootstrap.rank;
                    it->second->dst = dst;
                }
                else {
                    return -1;
                }
            }

            Transport::Request::BDelete *rem = it->second;

            // if there is space in the request packet, insert
            if (rem->count < max) {
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
    }

    return ds_id;
}

/**
 * Histogram
 * Places a Histogram request into the correct buffer for sending to a backend
 */
int Histogram(hxhim_t *hx,
              const std::size_t max,
              const int ds_id,
              Transport::Request::BHistogram *local,
              std::map<int, Transport::Request::BHistogram *> &remote) {
    if (ds_id > -1) {
        // split the backend id into destination rank and ds_offset
        const int dst = hxhim::datastore::get_rank(hx, ds_id);
        const int ds_offset = hxhim::datastore::get_offset(hx, ds_id);

        // group local keys
        if (dst == hx->p->bootstrap.rank) {
            // only add the destination if there is space
            if (local->count < max) {
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
            REF(remote)::iterator it = remote.find(dst);

            // if the destination is not found, check if there is space for another one
            if (it == remote.end()) {
                // if there is space for a new destination, insert it
                if (remote.size() < (hx->p->memory_pools.requests->regions() / 2)) {
                    it = remote.insert(std::make_pair(dst, hx->p->memory_pools.requests->acquire<Transport::Request::BHistogram>(hx->p->memory_pools.arrays, hx->p->memory_pools.buffers, max))).first;
                    it->second->src = hx->p->bootstrap.rank;
                    it->second->dst = dst;
                }
                else {
                    return -1;
                }
            }

            Transport::Request::BHistogram *rem = it->second;

            if (rem->count < max) {
                rem->ds_offsets[rem->count] = ds_offset;
                rem->count++;
            }
            else {
                return -1;
            }
        }
    }

    return ds_id;
}

}
}
