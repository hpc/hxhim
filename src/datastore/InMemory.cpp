#include <ctime>
#include <sstream>
#include <stdexcept>

#include "datastore/InMemory.hpp"
#include "hxhim/private.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/Blob.hpp"
#include "utils/elapsed.h"
#include "utils/memory.hpp"

hxhim::datastore::InMemory::InMemory(hxhim_t *hx,
                                     Histogram::Histogram *hist,
                                     const std::string &exact_name)
    : Datastore(hx, 0, hist),
      db()
{
    Datastore::Open(exact_name);
}

hxhim::datastore::InMemory::InMemory(hxhim_t *hx,
                   const int id,
                   Histogram::Histogram *hist,
                   const std::string &basename)
    : Datastore(hx, id, hist),
      db()
{
    Datastore::Open(basename);
}

hxhim::datastore::InMemory::~InMemory() {
    Close();
}

bool hxhim::datastore::InMemory::OpenImpl(const std::string &) {
    return true;
}

void hxhim::datastore::InMemory::CloseImpl() {
    db.clear();
}

/**
 * BPut
 * Performs a bulk PUT in
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return pointer to a list of results
 */
Transport::Response::BPut *hxhim::datastore::InMemory::BPutImpl(Transport::Request::BPut *req) {
    Transport::Response::BPut *res = construct<Transport::Response::BPut>(req->count);

    struct timespec start = {};
    struct timespec end = {};

    for(std::size_t i = 0; i < req->count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i]->data(), req->subjects[i]->size(), req->predicates[i]->data(), req->predicates[i]->size(), &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        db[std::string((char *) key, key_len)] = std::string((char *) req->objects[i]->data(), req->objects[i]->size());
        clock_gettime(CLOCK_MONOTONIC, &end);

        res->orig.subjects[i]   = construct<ReferenceBlob>(req->orig.subjects[i], req->subjects[i]->size());
        res->orig.predicates[i] = construct<ReferenceBlob>(req->orig.predicates[i], req->predicates[i]->size());

        dealloc(key);

        stats.puts++;
        stats.put_times += nano(start, end);

        // always successful
        res->statuses[i] = HXHIM_SUCCESS;
    }

    res->count = req->count;

    stats.put_times += nano(start, end);

    return res;
}

/**
 * BGet
 * Performs a bulk GET in InMemory
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @return pointer to a list of results
 */
Transport::Response::BGet *hxhim::datastore::InMemory::BGetImpl(Transport::Request::BGet *req) {
    Transport::Response::BGet *res = construct<Transport::Response::BGet>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        struct timespec start = {};
        struct timespec end = {};
        std::string value_str;

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i]->data(), req->subjects[i]->size(), req->predicates[i]->data(), req->predicates[i]->size(), &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

        res->ds_offsets[i]      = req->ds_offsets[i];

        // object type was stored as a value, not address, so copy it to the response
        res->object_types[i]    = req->object_types[i];

        res->orig.subjects[i]   = construct<ReferenceBlob>(req->orig.subjects[i], req->subjects[i]->size());
        res->orig.predicates[i] = construct<ReferenceBlob>(req->orig.predicates[i], req->predicates[i]->size());

        res->statuses[i] = (it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR;

        // copy the object into the response
        if (res->statuses[i] == HXHIM_SUCCESS) {
            res->objects[i] = construct<RealBlob>(it->second.size(), it->second.data());
        }

        stats.gets++;
        stats.get_times += nano(start, end);
    }

    res->count = req->count;

    return res;
}

/**
 * BGetOp
 * Performs a GetOp in InMemory
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return pointer to a list of results
 */
Transport::Response::BGetOp *hxhim::datastore::InMemory::BGetOpImpl(Transport::Request::BGetOp *req) {
    Transport::Response::BGetOp *res = construct<Transport::Response::BGetOp>(0);

    for(std::size_t i = 0; i < req->count; i++) {
        struct timespec start = {};
        struct timespec end = {};

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i]->data(), req->subjects[i]->size(), req->predicates[i]->data(), req->predicates[i]->size(), &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        clock_gettime(CLOCK_MONOTONIC, &end);

        // add in the time to get the first key-value without adding to the counter
        stats.get_times += nano(start, end);

        dealloc(key);

        // all responses for this Op share a status
        res->statuses[i]     = (it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR;
        res->object_types[i] = req->object_types[i];
        res->num_recs[i]     = 0;
        res->subjects[i]     = alloc_array<Blob *>(res->num_recs[i]);
        res->predicates[i]   = alloc_array<Blob *>(res->num_recs[i]);
        res->objects[i]      = alloc_array<Blob *>(res->num_recs[i]);

        for(std::size_t j = 0; (j < req->num_recs[i]) && (it != db.end()); j++) {
            const std::string &k = it->first;
            const std::string &v = it->second;

            // get subject and predicate for sending back
            void *sub = nullptr, *pred = nullptr;
            std::size_t sub_len = 0, pred_len = 0;
            key_to_sp(k.data(), k.size(), &sub, &sub_len, &pred, &pred_len);
            res->subjects[i][j] = construct<RealBlob>(sub, sub_len);
            res->predicates[i][j] = construct<RealBlob>(pred, pred_len);

            res->objects[i][j] = construct<RealBlob>(alloc(v.size()), v.size());
            memcpy(res->objects[i][j]->data(), v.data(), v.size());

            // move to next iterator according to operation
            switch (req->ops[i]) {
                case hxhim_get_op_t::HXHIM_GET_NEXT:
                    it++;
                    break;
                case hxhim_get_op_t::HXHIM_GET_PREV:
                    it--;
                    break;
                case hxhim_get_op_t::HXHIM_GET_FIRST:
                    it++;
                    break;
                case hxhim_get_op_t::HXHIM_GET_LAST:
                    it--;
                    break;
                default:
                    break;
            }

            clock_gettime(CLOCK_MONOTONIC, &end);
            stats.gets++;
            stats.get_times += nano(start, end);
        }

    }

    return res;
}

/**
 * BDelete
 * Performs a bulk DELETE in InMemory
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @return pointer to a list of results
 */
Transport::Response::BDelete *hxhim::datastore::InMemory::BDeleteImpl(Transport::Request::BDelete *req) {
    Transport::Response::BDelete *res = construct<Transport::Response::BDelete>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i]->data(), req->subjects[i]->size(), req->predicates[i]->data(), req->predicates[i]->size(), &key, &key_len);

        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        if (it != db.end()) {
            db.erase(it);
            res->statuses[i] = HXHIM_SUCCESS;
        }
        else {
            res->statuses[i] = HXHIM_ERROR;
        }

        res->orig.subjects[i]   = construct<ReferenceBlob>(req->orig.subjects[i], req->subjects[i]->size());
        res->orig.predicates[i] = construct<ReferenceBlob>(req->orig.predicates[i], req->predicates[i]->size());

        dealloc(key);
    }

    res->count = req->count;

    return res;
}

/**
 * Sync
 * NOOP
 *
 * @return HXHIM_SUCCESS
 */
int hxhim::datastore::InMemory::SyncImpl() {
    return HXHIM_SUCCESS;
}
