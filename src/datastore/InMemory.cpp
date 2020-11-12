#include <cstring>
#include <sstream>
#include <stdexcept>

#include "datastore/InMemory.hpp"
#include "hxhim/Blob.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/memory.hpp"

datastore::InMemory::InMemory(const int rank,
                                     const int id,
                                     Histogram::Histogram *hist,
                                     const std::string &basename)
    : Datastore(rank, id, hist),
      db()
{
    Datastore::Open(basename);
}

datastore::InMemory::~InMemory() {
    Close();
}

bool datastore::InMemory::OpenImpl(const std::string &) {
    return true;
}

void datastore::InMemory::CloseImpl() {
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
Transport::Response::BPut *datastore::InMemory::BPutImpl(Transport::Request::BPut *req) {
    datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BPut *res = construct<Transport::Response::BPut>(req->count);

    std::size_t key_buffer_len = all_keys_size(req);
    char *key_buffer = (char *) alloc(key_buffer_len);
    char *key_buffer_start = key_buffer;

    for(std::size_t i = 0; i < req->count; i++) {
        // the current key address and length
        char *key = nullptr;
        std::size_t key_len = 0;

        key = sp_to_key(req->subjects[i], req->predicates[i], key_buffer, key_buffer_len, key_len);
        db[std::string((char *) key, key_len)] = (std::string) req->objects[i];
        event.size += key_len + req->objects[i].size();

        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size(), req->subjects[i].data_type());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size(), req->predicates[i].data_type());

        // always successful
        res->statuses[i] = DATASTORE_SUCCESS;
    }

    dealloc(key_buffer_start);

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
Transport::Response::BGet *datastore::InMemory::BGetImpl(Transport::Request::BGet *req) {
    datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BGet *res = construct<Transport::Response::BGet>(req->count);

    std::size_t key_buffer_len = all_keys_size(req);
    char *key_buffer = (char *) alloc(key_buffer_len);
    char *key_buffer_start = key_buffer;

    for(std::size_t i = 0; i < req->count; i++) {
        struct timespec start = {};
        struct timespec end = {};
        std::string value_str;

        std::size_t key_len = 0;
        char *key = sp_to_key(req->subjects[i], req->predicates[i], key_buffer, key_buffer_len, key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        clock_gettime(CLOCK_MONOTONIC, &end);

        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size(), req->subjects[i].data_type());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size(), req->predicates[i].data_type());

        res->statuses[i] = (it != db.end())?DATASTORE_SUCCESS:DATASTORE_ERROR;

        event.size += key_len;

        // copy the object into the response
        if (res->statuses[i] == DATASTORE_SUCCESS) {
            res->objects[i] = RealBlob(it->second.size(), it->second.data(), req->object_types[i]);

            event.size += res->objects[i].size();
        }
    }

    dealloc(key_buffer_start);

    res->count = req->count;

    event.time.end = ::Stats::now();
    stats.gets.emplace_back(event);

    return res;
}

static void BGetOp_copy_response(const std::map<std::string, std::string>::const_iterator &it,
                                 Transport::Request::BGetOp *req,
                                 Transport::Response::BGetOp *res,
                                 const std::size_t i,
                                 const std::size_t j,
                                 datastore::Datastore::Stats::Event &event) {
    const std::string &k = it->first;
    const std::string &v = it->second;

    // copy key into subject/predicate
    key_to_sp(k.data(), k.size(), res->subjects[i][j], res->predicates[i][j], true);
    res->subjects[i][j].set_type(req->subjects[i].data_type());
    res->predicates[i][j].set_type(req->predicates[i].data_type());

    // copy object
    res->objects[i][j] = RealBlob(alloc(v.size()), v.size(), req->object_types[i]);
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
Transport::Response::BGetOp *datastore::InMemory::BGetOpImpl(Transport::Request::BGetOp *req) {
    Transport::Response::BGetOp *res = construct<Transport::Response::BGetOp>(req->count);

    std::size_t key_buffer_len = all_keys_size(req);
    char *key_buffer = (char *) alloc(key_buffer_len);
    char *key_buffer_start = key_buffer;

    for(std::size_t i = 0; i < req->count; i++) {
        datastore::Datastore::Stats::Event event;
        event.time.start = ::Stats::now();

        decltype(db)::const_iterator it = db.end();

        // prepare response
        res->num_recs[i]     = 0;
        res->subjects[i]     = alloc_array<Blob>(req->num_recs[i]);
        res->predicates[i]   = alloc_array<Blob>(req->num_recs[i]);
        res->objects[i]      = alloc_array<Blob>(req->num_recs[i]);

        if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_EQ) {
            std::size_t key_len = 0;
            char *key = sp_to_key(req->subjects[i], req->predicates[i], key_buffer, key_buffer_len, key_len);

            it = db.find(std::string((char *) key, key_len));

            if (it != db.end()) {
                // only 1 response, so j == 0 (num_recs is ignored)
                BGetOp_copy_response(it, req, res, i, 0, event);
                res->statuses[i] = DATASTORE_SUCCESS;
            }
            else {
                BGetOp_error_response(res, i, req->subjects[i], req->predicates[i], event);

                res->statuses[i] = DATASTORE_ERROR;
            }
        }
        else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_NEXT) {
            std::size_t key_len = 0;
            char *key = sp_to_key(req->subjects[i], req->predicates[i], key_buffer, key_buffer_len, key_len);

            it = db.find(std::string((char *) key, key_len));

            if (it != db.end()) {
                // first result returned is (subject, predicate)
                // (results are offsets)
                for(std::size_t j = 0; (j < req->num_recs[i]) && (it != db.end()); j++) {
                    BGetOp_copy_response(it, req, res, i, j, event);
                    it++;
                }

                // all responses for this Op share a status
                res->statuses[i] = DATASTORE_SUCCESS;
            }
            else {
                BGetOp_error_response(res, i, req->subjects[i], req->predicates[i], event);

                res->statuses[i] = DATASTORE_ERROR;
            }
        }
        else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_PREV) {
            std::size_t key_len = 0;
            char *key = sp_to_key(req->subjects[i], req->predicates[i], key_buffer, key_buffer_len, key_len);

            it = db.find(std::string((char *) key, key_len));

            if (it != db.end()) {
                // first result returned is (subject, predicate)
                // (results are offsets)
                for(std::size_t j = 0; j < req->num_recs[i]; j++) {
                    BGetOp_copy_response(it, req, res, i, j, event);
                    if (it == db.begin()) {
                        break;
                    }
                    it--;
                }

                res->statuses[i] = DATASTORE_SUCCESS;
            }
            else {
                BGetOp_error_response(res, i, req->subjects[i], req->predicates[i], event);

                res->statuses[i] = DATASTORE_ERROR;
            }
        }
        else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_FIRST) {
            // ignore key
            it = db.begin();

            if (it != db.end()) {
                for(std::size_t j = 0; (j < req->num_recs[i]) && (it != db.end()); j++) {
                    BGetOp_copy_response(it, req, res, i, j, event);
                    it++;
                }

                res->statuses[i] = DATASTORE_SUCCESS;
            }
            else {
                BGetOp_error_response(res, i, req->subjects[i], req->predicates[i], event);

                res->statuses[i] = DATASTORE_ERROR;
            }
        }
        else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_LAST) {
            // ignore key
            it = db.end();
            it--;

            if (it != db.end()) {
                for(std::size_t j = 0; j < req->num_recs[i]; j++) {
                    BGetOp_copy_response(it, req, res, i, j, event);
                    if (it == db.begin()) {
                        break;
                    }
                    it--;
                }

                res->statuses[i] = DATASTORE_SUCCESS;
            }
            else {
                BGetOp_error_response(res, i, req->subjects[i], req->predicates[i], event);

                res->statuses[i] = DATASTORE_ERROR;
            }
        }

        res->count++;

        event.count = res->num_recs[i];
        event.time.end = ::Stats::now();
        stats.gets.emplace_back(event);
    }

    dealloc(key_buffer_start);

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
Transport::Response::BDelete *datastore::InMemory::BDeleteImpl(Transport::Request::BDelete *req) {
    datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BDelete *res = construct<Transport::Response::BDelete>(req->count);

    std::size_t key_buffer_len = all_keys_size(req);
    char *key_buffer = (char *) alloc(key_buffer_len);
    char *key_buffer_start = key_buffer;

    for(std::size_t i = 0; i < req->count; i++) {
        std::size_t key_len = 0;
        char *key = sp_to_key(req->subjects[i], req->predicates[i], key_buffer, key_buffer_len, key_len);

        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        if (it != db.end()) {
            db.erase(it);
            res->statuses[i] = DATASTORE_SUCCESS;
        }
        else {
            res->statuses[i] = DATASTORE_ERROR;
        }

        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size(), req->subjects[i].data_type());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size(), req->predicates[i].data_type());

        event.size += key_len;
    }

    dealloc(key_buffer_start);

    res->count = req->count;

    event.time.end = ::Stats::now();
    stats.deletes.emplace_back(event);

    return res;
}

/**
 * Sync
 * NOOP
 *
 * @return DATASTORE_SUCCESS
 */
int datastore::InMemory::SyncImpl() {
    return DATASTORE_SUCCESS;
}
