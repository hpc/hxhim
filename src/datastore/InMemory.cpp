#include <cstring>
#include <sstream>
#include <stdexcept>

#include "datastore/InMemory.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"
#include "utils/triplestore.hpp"

hxhim::datastore::InMemory::InMemory(const int rank,
                                     const int offset,
                                     const int id,
                                     Histogram::Histogram *hist,
                                     const std::string &basename)
    : Datastore(rank, offset, id, hist),
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
    hxhim::datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BPut *res = construct<Transport::Response::BPut>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        // SPO
        {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(req->subjects[i], req->predicates[i], &key, &key_len);

            db[std::string((char *) key, key_len)] = (std::string) req->objects[i];
            dealloc(key);

            event.size += key_len + req->objects[i].size();
        }

        #if SOP
        {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(req->subjects[i], req->objects[i], &key, &key_len);

            db[std::string((char *) key, key_len)] = (std::string) req->predicates[i];
            dealloc(key);

            event.size += key_len + req->predicates[i].size();
        }
        #endif

        #if PSO
        {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(req->predicates[i], req->subjects[i], &key, &key_len);

            db[std::string((char *) key, key_len)] = (std::string) req->objects[i];
            dealloc(key);

            event.size += key_len + req->objects[i].size();
        }
        #endif

        #if POS
        {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(req->predicates[i], req->objects[i], &key, &key_len);

            db[std::string((char *) key, key_len)] = (std::string) req->subjects[i];
            dealloc(key);

            event.size += key_len + req->subjects[i].size();
        }
        #endif

        #if OSP
        {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(req->objects[i], req->subjects[i], &key, &key_len);

            db[std::string((char *) key, key_len)] = (std::string) req->predicates[i];
            dealloc(key);

            event.size += key_len + req->predicates[i].size();
        }
        #endif

        #if OPS
        {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(req->objects[i], req->predicates[i], &key, &key_len);

            db[std::string((char *) key, key_len)] = (std::string) req->subjects[i];
            dealloc(key);

            event.size += key_len + req->subjects[i].size();
        }
        #endif

        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size());

        // always successful
        res->statuses[i] = HXHIM_SUCCESS;
    }

    res->count = req->count;

    event.time.end = ::Stats::now();
    stats.puts.emplace_back(event);

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
    hxhim::datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BGet *res = construct<Transport::Response::BGet>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        struct timespec start = {};
        struct timespec end = {};
        std::string value_str;

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i], req->predicates[i], &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

        res->ds_offsets[i]      = req->ds_offsets[i];

        // object type was stored as a value, not address, so copy it to the response
        res->object_types[i]    = req->object_types[i];

        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size());

        res->statuses[i] = (it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR;

        event.size += key_len;

        // copy the object into the response
        if (res->statuses[i] == HXHIM_SUCCESS) {
            res->objects[i] = RealBlob(it->second.size(), it->second.data());

            event.size += res->objects[i].size();
        }
    }

    res->count = req->count;

    event.time.end = ::Stats::now();
    stats.gets.emplace_back(event);

    return res;
}

static void BGetOp_copy_response(const std::map<std::string, std::string>::const_iterator &it,
                                 Transport::Response::BGetOp *res,
                                 const std::size_t i,
                                 const std::size_t j,
                                 hxhim::datastore::Datastore::Stats::Event &event) {
    const std::string &k = it->first;
    const std::string &v = it->second;

    // copy key into subject/predicate
    key_to_sp(k.data(), k.size(), res->subjects[i][j], res->predicates[i][j], true);

    // copy object
    res->objects[i][j] = RealBlob(alloc(v.size()), v.size());
    memcpy(res->objects[i][j].data(), v.data(), v.size());

    res->num_recs[i]++;

    event.size += k.size() + res->objects[i][j].size();
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
    Transport::Response::BGetOp *res = construct<Transport::Response::BGetOp>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        hxhim::datastore::Datastore::Stats::Event event;
        event.time.start = ::Stats::now();

        decltype(db)::const_iterator it = db.end();

        // prepare response
        res->object_types[i] = req->object_types[i];
        res->num_recs[i]     = 0;
        res->subjects[i]     = alloc_array<Blob>(req->num_recs[i]);
        res->predicates[i]   = alloc_array<Blob>(req->num_recs[i]);
        res->objects[i]      = alloc_array<Blob>(req->num_recs[i]);

        if (req->ops[i] == hxhim_get_op_t::HXHIM_GET_EQ) {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(req->subjects[i], req->predicates[i], &key, &key_len);

            it = db.find(std::string((char *) key, key_len));

            dealloc(key);

            res->statuses[i] = (it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR;

            // only 1 response, so j == 0 (num_recs is ignored)
            BGetOp_copy_response(it, res, i, 0, event);
        }
        else if (req->ops[i] == hxhim_get_op_t::HXHIM_GET_NEXT) {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(req->subjects[i], req->predicates[i], &key, &key_len);

            it = db.find(std::string((char *) key, key_len));

            dealloc(key);

            // all responses for this Op share a status
            res->statuses[i] = (it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR;

            // first result returned is (subject, predicate)
            // (results are offsets)
            for(std::size_t j = 0; (j < req->num_recs[i]) && (it != db.end()); j++) {
                BGetOp_copy_response(it, res, i, j, event);
                it++;
            }
        }
        else if (req->ops[i] == hxhim_get_op_t::HXHIM_GET_PREV) {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(req->subjects[i], req->predicates[i], &key, &key_len);

            it = db.find(std::string((char *) key, key_len));

            dealloc(key);

            if (it != db.end()) {
                // first result returned is (subject, predicate)
                // (results are offsets)
                for(std::size_t j = 0; j < req->num_recs[i]; j++) {
                    BGetOp_copy_response(it, res, i, j, event);
                    if (it == db.begin()) {
                        break;
                    }
                    it--;
                }

                res->statuses[i] = HXHIM_SUCCESS;
            }
            else {
                res->statuses[i] = HXHIM_ERROR;
            }
        }
        else if (req->ops[i] == hxhim_get_op_t::HXHIM_GET_FIRST) {
            // ignore key
            it = db.begin();

            if (it != db.end()) {
                for(std::size_t j = 0; (j < req->num_recs[i]) && (it != db.end()); j++) {
                    BGetOp_copy_response(it, res, i, j, event);
                    it++;
                }

                res->statuses[i] = HXHIM_SUCCESS;
            }
            else {
                res->statuses[i] = HXHIM_ERROR;
            }
        }
        else if (req->ops[i] == hxhim_get_op_t::HXHIM_GET_LAST) {
            // ignore key
            it = db.end();
            it--;

            if (it != db.end()) {
                for(std::size_t j = 0; j < req->num_recs[i]; j++) {
                    BGetOp_copy_response(it, res, i, j, event);
                    if (it == db.begin()) {
                        break;
                    }
                    it--;
                }

                res->statuses[i] = HXHIM_SUCCESS;
            }
            else {
                res->statuses[i] = HXHIM_ERROR;
            }
        }

        res->count++;

        event.count = res->num_recs[i];
        event.time.end = ::Stats::now();
        stats.gets.emplace_back(event);
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
    hxhim::datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BDelete *res = construct<Transport::Response::BDelete>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i], req->predicates[i], &key, &key_len);

        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        if (it != db.end()) {
            db.erase(it);
            res->statuses[i] = HXHIM_SUCCESS;
        }
        else {
            res->statuses[i] = HXHIM_ERROR;
        }

        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size());

        dealloc(key);

        event.size += key_len;
    }

    res->count = req->count;

    event.time.end = ::Stats::now();
    stats.deletes.emplace_back(event);

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
