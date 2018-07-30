#include "hxhim/private.hpp"
#include "hxhim/shuffle.hpp"

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
        Transport::Request::BPut **remote) {
    // get the destination backend id for the key
    const int backend_id = hx->p->hash(subject, subject_len, predicate, predicate_len, hx->p->hash_args);

    if (backend_id > -1) {
        // split the backend id into destination rank and db_offset
        const int dst = backend_id;
        const int db_offset = 0;

        // group local keys
        if (dst == hx->mpi.rank) {
            if (local->count >= max) {
                return -1;
            }

            local->db_offsets[local->count] = db_offset;
            local->subjects[local->count] = subject;
            local->subject_lens[local->count] = subject_len;
            local->predicates[local->count] = predicate;
            local->predicate_lens[local->count] = predicate_len;
            local->object_types[local->count] = object_type;
            local->objects[local->count] = object;
            local->object_lens[local->count] = object_len;
            local->count++;
        }
        // group remote keys
        else {
            Transport::Request::BPut *&rem = remote[dst];

            // if there were no previous keys going to this destination, set the initial values
            if (!rem) {
                rem = new Transport::Request::BPut(max);
                rem->src = hx->mpi.rank;
                rem->dst = dst;
            }

            if (rem->count >= max) {
                return -1;
            }

            rem->db_offsets[rem->count] = db_offset;
            rem->subjects[rem->count] = subject;
            rem->subject_lens[rem->count] = subject_len;
            rem->predicates[rem->count] = predicate;
            rem->predicate_lens[rem->count] = predicate_len;
            rem->object_types[rem->count] = object_type;
            rem->objects[rem->count] = object;
            rem->object_lens[rem->count] = object_len;
            rem->count++;
        }
    }

    return backend_id;
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
        Transport::Request::BGet **remote) {
    // get the destination backend id for the key
    const int backend_id = hx->p->hash(subject, subject_len, predicate, predicate_len, hx->p->hash_args);

    if (backend_id > -1) {
        // split the backend id into destination rank and db_offset
        const int dst = backend_id;
        const int db_offset = 0;

        // group local keys
        if (dst == hx->mpi.rank) {
            local->db_offsets[local->count] = db_offset;
            local->subjects[local->count] = subject;
            local->subject_lens[local->count] = subject_len;
            local->predicates[local->count] = predicate;
            local->predicate_lens[local->count] = predicate_len;
            local->object_types[local->count] = object_type;
            local->count++;
        }
        // group remote keys
        else {
            Transport::Request::BGet *&rem = remote[dst];

            // if there were no previous keys going to this destination, set the initial values
            if (!rem) {
                rem = new Transport::Request::BGet(max);
                rem->src = hx->mpi.rank;
                rem->dst = dst;
            }

            rem->db_offsets[rem->count] = db_offset;
            rem->subjects[rem->count] = subject;
            rem->subject_lens[rem->count] = subject_len;
            rem->predicates[rem->count] = predicate;
            rem->predicate_lens[rem->count] = predicate_len;
            rem->object_types[rem->count] = object_type;
            rem->count++;
        }
    }

    return backend_id;
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
          Transport::Request::BGetOp **remote) {
    // get the destination backend id for the key
    const int backend_id = hx->p->hash(subject, subject_len, predicate, predicate_len, hx->p->hash_args);

    if (backend_id > -1) {
        // split the backend id into destination rank and db_offset
        const int dst = backend_id;
        const int db_offset = 0;

        // group local keys
        if (dst == hx->mpi.rank) {
            local->db_offsets[local->count] = db_offset;
            local->subjects[local->count] = subject;
            local->subject_lens[local->count] = subject_len;
            local->predicates[local->count] = predicate;
            local->predicate_lens[local->count] = predicate_len;
            local->object_types[local->count] = object_type;
            local->num_recs[local->count] = recs;
            local->ops[local->count] = op;
            local->count++;
        }
        // group remote keys
        else {
            Transport::Request::BGetOp *&rem = remote[dst];

            // if there were no previous keys going to this destination, set the initial values
            if (!rem) {
                rem = new Transport::Request::BGetOp(max);
                rem->src = hx->mpi.rank;
                rem->dst = dst;
            }

            rem->db_offsets[rem->count] = db_offset;
            rem->subjects[rem->count] = subject;
            rem->subject_lens[rem->count] = subject_len;
            rem->predicates[rem->count] = predicate;
            rem->predicate_lens[rem->count] = predicate_len;
            rem->object_types[rem->count] = object_type;
            rem->num_recs[rem->count] = recs;
            rem->ops[rem->count] = op;
            rem->count++;
        }
    }

    return backend_id;
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
           Transport::Request::BDelete **remote) {
    // get the destination backend id for the key
    const int backend_id = hx->p->hash(subject, subject_len, predicate, predicate_len, hx->p->hash_args);

    if (backend_id > -1) {
        // split the backend id into destination rank and db_offset
        const int dst = backend_id;
        const int db_offset = 0;

        // group local keys
        if (dst == hx->mpi.rank) {
            if (local->count >= max) {
                return -1;
            }

            local->db_offsets[local->count] = db_offset;
            local->subjects[local->count] = subject;
            local->subject_lens[local->count] = subject_len;
            local->predicates[local->count] = predicate;
            local->predicate_lens[local->count] = predicate_len;
            local->count++;
        }
        // group remote keys
        else {
            Transport::Request::BDelete *&rem = remote[dst];

            // if there were no previous keys going to this destination, set the initial values
            if (!rem) {
                rem = new Transport::Request::BDelete(max);
                rem->src = hx->mpi.rank;
                rem->dst = dst;
            }

            if (rem->count >= max) {
                return -1;
            }

            rem->db_offsets[rem->count] = db_offset;
            rem->subjects[rem->count] = subject;
            rem->subject_lens[rem->count] = subject_len;
            rem->predicates[rem->count] = predicate;
            rem->predicate_lens[rem->count] = predicate_len;
            rem->count++;
        }
    }

    return backend_id;
}

}
}
