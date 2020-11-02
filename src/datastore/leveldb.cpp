#include <cerrno>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>

#include "leveldb/write_batch.h"

#include "hxhim/accessors.hpp"
#include "datastore/leveldb.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"
#include "utils/mkdir_p.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"
#include "utils/triplestore.hpp"

datastore::leveldb::leveldb(const int rank,
                                   Histogram::Histogram *hist,
                                   const std::string &exact_name,
                                   const bool create_if_missing)
    : Datastore(rank, 0, hist),
      dbname(exact_name),
      create_if_missing(create_if_missing),
      db(nullptr), options()
{
    options.create_if_missing = create_if_missing;

    Datastore::Open(dbname);

    mlog(LEVELDB_INFO, "Opened leveldb with name: %s", exact_name.c_str());
}

datastore::leveldb::leveldb(const int rank,
                                   const int id,
                                   Histogram::Histogram *hist,
                                   const std::string &prefix,
                                   const std::string &basename,
                                   const bool create_if_missing)
    : Datastore(rank, id, hist),
      dbname(),
      create_if_missing(create_if_missing),
      db(nullptr), options()
{
    mkdir_p(prefix, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    std::stringstream s;
    s << prefix << "/" << basename << "-" << id;
    dbname = s.str();

    options.create_if_missing = create_if_missing;

    #if PRINT_TIMESTAMPS
    ::Stats::Chronostamp init_open;
    init_open.start = ::Stats::now();
    #endif
    Datastore::Open(dbname);
    mlog(LEVELDB_INFO, "Opened leveldb with name: %s", dbname.c_str());
    #if PRINT_TIMESTAMPS
    init_open.end = ::Stats::now();
    ::Stats::print_event(std::cerr, rank, "hxhim_leveldb_open", ::Stats::global_epoch, init_open);
    #endif
}

datastore::leveldb::~leveldb() {
    Close();
}

const std::string &datastore::leveldb::name() const {
    return dbname;
}

bool datastore::leveldb::OpenImpl(const std::string &new_name) {
    #if PRINT_TIMESTAMPS
    ::Stats::Chronostamp leveldb_open;
    leveldb_open.start = ::Stats::now();
    #endif
    ::leveldb::Status status = ::leveldb::DB::Open(options, new_name, &db);
    #if PRINT_TIMESTAMPS
    leveldb_open.end = ::Stats::now();
    #endif
    if (!status.ok()) {
        throw std::runtime_error("Could not configure leveldb datastore " + new_name + ": " + status.ToString());
    }
    #if PRINT_TIMESTAMPS
    ::Stats::print_event(std::cerr, rank, "leveldb_open", ::Stats::global_epoch, leveldb_open);
    #endif
    return status.ok();
}

void datastore::leveldb::CloseImpl() {
    delete db;
    db = nullptr;
}

/**
 * BPut
 * Performs a bulk PUT in leveldb
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return pointer to a list of results
 */
Transport::Response::BPut *datastore::leveldb::BPutImpl(Transport::Request::BPut *req) {
    mlog(LEVELDB_INFO, "LevelDB BPut");

    datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BPut *res = construct<Transport::Response::BPut>(req->count);

    std::size_t key_buffer_len = all_keys_size(req);
    char *key_buffer = (char *) alloc(key_buffer_len);
    char *key_buffer_start = key_buffer;

    memset(key_buffer, 0, key_buffer_len);

    // batch up PUTs
    ::leveldb::WriteBatch batch;
    for(std::size_t i = 0; i < req->count; i++) {
        // the current key address and length
        char *key = nullptr;
        std::size_t key_len = 0;

        // SPO
        {
            key = sp_to_key(req->subjects[i], req->predicates[i], key_buffer, key_buffer_len, key_len);
            batch.Put(::leveldb::Slice(key, key_len), ::leveldb::Slice((char *) req->objects[i].data(), req->objects[i].size()));
            event.size += key_len + req->objects[i].size();
        }

        #if SOP
        {
            key = sp_to_key(req->subjects[i], req->objects[i], key_buffer, key_buffer_len, key_len);
            batch.Put(::leveldb::Slice(key, key_len), ::leveldb::Slice((char *) req->predicates[i].data(), req->predicates[i].size()));
            event.size += key_len + req->predicates[i].size();
        }
        #endif

        #if PSO
        {
            key = sp_to_key(req->predicates[i], req->subjects[i], key_buffer, key_buffer_len, key_len);
            batch.Put(::leveldb::Slice(key, key_len), ::leveldb::Slice((char *) req->objects[i].data(), req->objects[i].size()));
            event.size += key_len + req->objects[i].size();
        }
        #endif

        #if POS
        {
            key = sp_to_key(req->predicates[i], req->objects[i], key_buffer, key_buffer_len, key_len);
            batch.Put(::leveldb::Slice(key, key_len), ::leveldb::Slice((char *) req->subjects[i].data(), req->subjects[i].size()));
            event.size += key_len + req->subjects[i].size();
        }
        #endif

        #if OSP
        {
            key = sp_to_key(req->objects[i], req->subjects[i], key_buffer, key_buffer_len, key_len);
            batch.Put(::leveldb::Slice(key, key_len), ::leveldb::Slice((char *) req->predicates[i].data(), req->predicates[i].size()));
            event.size += key_len + req->predicates[i].size();
        }
        #endif

        #if OPS
        {
            key = sp_to_key(req->objects[i], req->predicates[i], key_buffer, key_buffer_len, key_len);
            batch.Put(::leveldb::Slice(key, key_len), ::leveldb::Slice((char *) req->subjects[i].data(), req->subjects[i].size()));
            event.size += key_len + req->subjects[i].size();
        }
        #endif

        // save requesting addresses for sending back
        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size());
    }

    // add in the time to write the key-value pairs without adding to the counter
    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);

    dealloc(key_buffer_start);

    if (status.ok()) {
        for(std::size_t i = 0; i < req->count; i++) {
            res->statuses[i] = DATASTORE_SUCCESS;
        }
        mlog(LEVELDB_INFO, "LevelDB write success");
    }
    else {
        for(std::size_t i = 0; i < req->count; i++) {
            res->statuses[i] = DATASTORE_ERROR;
        }
        mlog(LEVELDB_INFO, "LevelDB write error");
    }

    res->count = req->count;
    event.time.end = ::Stats::now();
    stats.puts.emplace_back(event);

    mlog(LEVELDB_INFO, "LevelDB BPut Completed");
    return res;
}

/**
 * BGet
 * Performs a bulk GET in leveldb
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @return pointer to a list of results
 */
Transport::Response::BGet *datastore::leveldb::BGetImpl(Transport::Request::BGet *req) {
    mlog(LEVELDB_INFO, "Rank %d LevelDB GET processing %zu item in %ps", rank, req->count, req);

    datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BGet *res = construct<Transport::Response::BGet>(req->count);

    std::size_t key_buffer_len = all_keys_size(req);
    char *key_buffer = (char *) alloc(key_buffer_len);
    char *key_buffer_start = key_buffer;

    // batch up GETs
    for(std::size_t i = 0; i < req->count; i++) {
        mlog(LEVELDB_INFO, "Rank %d LevelDB GET processing %p[%zu] = {%p, %p}", rank, req, i, req->subjects[i].data(), req->predicates[i].data());

        std::size_t key_len = 0;
        char *key = sp_to_key(req->subjects[i], req->predicates[i], key_buffer, key_buffer_len, key_len);

        // create the key
        const ::leveldb::Slice k(key, key_len);

        // get the value
        ::leveldb::Slice value; // read gotten value into a Slice instead of a std::string to save a few copies
        ::leveldb::Status status = db->Get(::leveldb::ReadOptions(), k, value);

        // object type was stored as a value, not address, so copy it to the response
        res->object_types[i]    = req->object_types[i];

        // save requesting addresses for sending back
        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size());
        event.size += key_len;

        // put object into response
        if (status.ok()) {
            mlog(LEVELDB_INFO, "Rank %d LevelDB GET success", rank);
            res->statuses[i] = DATASTORE_SUCCESS;
            res->objects[i] = RealBlob(value.size(), value.data());

            event.size += res->objects[i].size();
        }
        else {
            mlog(LEVELDB_INFO, "Rank %d LevelDB GET error: %s", rank, status.ToString().c_str());
            res->statuses[i] = DATASTORE_ERROR;
        }

        res->count++;
    }

    dealloc(key_buffer_start);

    event.time.end = ::Stats::now();
    stats.gets.emplace_back(event);

    mlog(LEVELDB_INFO, "Rank %d LevelDB GET done processing %p", rank, req);
    return res;
}

static void BGetOp_copy_response(const ::leveldb::Iterator *it,
                                 Transport::Response::BGetOp *res,
                                 const std::size_t i,
                                 const std::size_t j,
                                 datastore::Datastore::Stats::Event &event) {
    const ::leveldb::Slice k = it->key();
    const ::leveldb::Slice v = it->value();

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
 * Performs a GetOp in leveldb
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return pointer to a list of results
 */
Transport::Response::BGetOp *datastore::leveldb::BGetOpImpl(Transport::Request::BGetOp *req) {
    Transport::Response::BGetOp *res = construct<Transport::Response::BGetOp>(req->count);

    std::size_t key_buffer_len = all_keys_size(req);
    char *key_buffer = (char *) alloc(key_buffer_len);
    char *key_buffer_start = key_buffer;

    ::leveldb::Iterator *it = db->NewIterator(::leveldb::ReadOptions());

    for(std::size_t i = 0; i < req->count; i++) {
        datastore::Datastore::Stats::Event event;
        event.time.start = ::Stats::now();

        // prepare response
        res->object_types[i] = req->object_types[i];
        res->num_recs[i]     = 0;
        res->subjects[i]     = alloc_array<Blob>(req->num_recs[i]);
        res->predicates[i]   = alloc_array<Blob>(req->num_recs[i]);
        res->objects[i]      = alloc_array<Blob>(req->num_recs[i]);

        if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_EQ) {
            std::size_t key_len = 0;
            char *key = sp_to_key(req->subjects[i], req->predicates[i], key_buffer, key_buffer_len, key_len);

            it->Seek(::leveldb::Slice(key, key_len));

            if (it->Valid()) {
                // only 1 response, so j == 0 (num_recs is ignored)
                BGetOp_copy_response(it, res, i, 0, event);

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

            it->Seek(::leveldb::Slice(key, key_len));

            if (it->Valid()) {
                // first result returned is (subject, predicate)
                // (results are offsets)
                for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                    BGetOp_copy_response(it, res, i, j, event);
                    it->Next();
                }

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

            it->Seek(::leveldb::Slice(key, key_len));

            if (it->Valid()) {
                // first result returned is (subject, predicate)
                // (results are offsets)
                for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                    BGetOp_copy_response(it, res, i, j, event);
                    it->Prev();
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
            it->SeekToFirst();

            if (it->Valid()) {
                // first result returned is (subject, predicate)
                // (results are offsets)
                for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                    BGetOp_copy_response(it, res, i, j, event);
                    it->Next();
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
            it->SeekToLast();

            if (it->Valid()) {
                // first result returned is (subject, predicate)
                // (results are offsets)
                for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                    BGetOp_copy_response(it, res, i, j, event);
                    it->Prev();
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
        stats.getops.emplace_back(event);
    }

    dealloc(key_buffer_start);

    delete it;

    return res;
}

/**
 * BDelete
 * Performs a bulk DELETE in leveldb
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @return pointer to a list of results
 */
Transport::Response::BDelete *datastore::leveldb::BDeleteImpl(Transport::Request::BDelete *req) {
    datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BDelete *res = construct<Transport::Response::BDelete>(req->count);

    std::size_t key_buffer_len = all_keys_size(req);
    char *key_buffer = (char *) alloc(key_buffer_len);
    char *key_buffer_start = key_buffer;

    ::leveldb::WriteBatch batch;

    // batch delete
    for(std::size_t i = 0; i < req->count; i++) {
        std::size_t key_len = 0;
        char *key = sp_to_key(req->subjects[i], req->predicates[i], key_buffer, key_buffer_len, key_len);

        batch.Delete(::leveldb::Slice(key, key_len));

        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size());

        event.size += key_len;
    }

    // create responses
    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);

    dealloc(key_buffer_start);

    const int stat = status.ok()?DATASTORE_SUCCESS:DATASTORE_ERROR;
    for(std::size_t i = 0; i < req->count; i++) {
        res->statuses[i] = stat;
    }

    res->count = req->count;

    event.time.end = ::Stats::now();
    stats.deletes.emplace_back(event);

    return res;
}

/**
 * Sync
 * Syncs the database to disc
 *
 * @return DATASTORE_SUCCESS or DATASTORE_ERROR on error
 */
int datastore::leveldb::SyncImpl() {
    ::leveldb::WriteBatch batch;
    ::leveldb::WriteOptions options;
    options.sync = true;
    return db->Write(options, &batch).ok()?DATASTORE_SUCCESS:DATASTORE_ERROR;
}
