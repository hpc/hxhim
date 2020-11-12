#include <cerrno>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>

#include "rocksdb/write_batch.h"

#include "datastore/rocksdb.hpp"
#include "hxhim/Blob.hpp"
#include "hxhim/accessors.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/memory.hpp"
#include "utils/mkdir_p.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

datastore::rocksdb::rocksdb(const int rank,
                            Transform::Callbacks *callbacks,
                            Histogram::Histogram *hist,
                            const std::string &exact_name,
                            const bool create_if_missing)
    : Datastore(rank, 0, callbacks, hist),
      dbname(exact_name),
      create_if_missing(create_if_missing),
      db(nullptr), options()
{
    options.create_if_missing = create_if_missing;

    Datastore::Open(dbname);

    mlog(ROCKSDB_INFO, "Opened rocksdb with name: %s", exact_name.c_str());
}

datastore::rocksdb::rocksdb(const int rank,
                            const int id,
                            Transform::Callbacks *callbacks,
                            Histogram::Histogram *hist,
                            const std::string &prefix,
                            const std::string &basename,
                            const bool create_if_missing)
    : Datastore(rank, id, callbacks, hist),
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
    mlog(ROCKSDB_INFO, "Opened rocksdb with name: %s", dbname.c_str());
    #if PRINT_TIMESTAMPS
    init_open.end = ::Stats::now();
    ::Stats::print_event(std::cerr, rank, "hxhim_rocksdb_open", ::Stats::global_epoch, init_open);
    #endif
}

datastore::rocksdb::~rocksdb() {
    Close();
}

const std::string &datastore::rocksdb::name() const {
    return dbname;
}

bool datastore::rocksdb::OpenImpl(const std::string &new_name) {
    #if PRINT_TIMESTAMPS
    ::Stats::Chronostamp rocksdb_open;
    rocksdb_open.start = ::Stats::now();
    #endif
    ::rocksdb::Status status = ::rocksdb::DB::Open(options, new_name, &db);
    #if PRINT_TIMESTAMPS
    rocksdb_open.end = ::Stats::now();
    #endif
    if (!status.ok()) {
        throw std::runtime_error("Could not configure rocksdb datastore " + new_name + ": " + status.ToString());
    }
    #if PRINT_TIMESTAMPS
    ::Stats::print_event(std::cerr, rank, "rocksdb_open", ::Stats::global_epoch, rocksdb_open);
    #endif
    return status.ok();
}

void datastore::rocksdb::CloseImpl() {
    delete db;
    db = nullptr;
}

/**
 * BPut
 * Performs a bulk PUT in rocksdb
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return pointer to a list of results
 */
Transport::Response::BPut *datastore::rocksdb::BPutImpl(Transport::Request::BPut *req) {
    mlog(ROCKSDB_INFO, "Rocksdb BPut");

    datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BPut *res = construct<Transport::Response::BPut>(req->count);

    // batch up PUTs
    ::rocksdb::WriteBatch batch;
    for(std::size_t i = 0; i < req->count; i++) {
        void *subject = nullptr;
        std::size_t subject_len = 0;
        void *predicate = nullptr;
        std::size_t predicate_len = 0;
        void *object = nullptr;
        std::size_t object_len = 0;

        int status = DATASTORE_ERROR; // successful batching will set toe DATASTORE_UNSET
        if ((encode(req->subjects[i],   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (encode(req->predicates[i], &predicate, &predicate_len) == DATASTORE_SUCCESS) &&
            (encode(req->objects[i],    &object,    &object_len)    == DATASTORE_SUCCESS)) {
            // the current key address and length
            std::string key;
            sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                      ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                      key);

            batch.Put(::rocksdb::Slice(key.c_str(), key.size()), ::rocksdb::Slice((char *) object, object_len));

            event.size += key.size() + object_len;
            status = DATASTORE_UNSET;
        }

        res->statuses[i] = status;

        if (subject != req->subjects[i].data()) {
            dealloc(subject);
        }
        if (predicate != req->predicates[i].data()) {
            dealloc(predicate);
        }
        if (object != req->objects[i].data()) {
            dealloc(object);
        }

        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size(), req->subjects[i].data_type());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size(), req->predicates[i].data_type());
    }

    ::rocksdb::Status status = db->Write(::rocksdb::WriteOptions(), &batch);

    if (status.ok()) {
        // successful writes only update statuses that were unset
        for(std::size_t i = 0; i < req->count; i++) {
            if (res->statuses[i] == DATASTORE_UNSET) {
                res->statuses[i] = DATASTORE_SUCCESS;
            }
        }
        mlog(ROCKSDB_INFO, "Rocksdb write success");
    }
    else {
        for(std::size_t i = 0; i < req->count; i++) {
            res->statuses[i] = DATASTORE_ERROR;
        }
        mlog(ROCKSDB_INFO, "Rocksdb write error");
    }

    res->count = req->count;
    event.time.end = ::Stats::now();
    stats.puts.emplace_back(event);

    mlog(ROCKSDB_INFO, "Rocksdb BPut Completed");
    return res;
}

/**
 * BGet
 * Performs a bulk GET in rocksdb
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @return pointer to a list of results
 */
Transport::Response::BGet *datastore::rocksdb::BGetImpl(Transport::Request::BGet *req) {
    mlog(ROCKSDB_INFO, "Rank %d Rocksdb GET processing %zu item in %ps", rank, req->count, req);

    datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BGet *res = construct<Transport::Response::BGet>(req->count);

    // batch up GETs
    for(std::size_t i = 0; i < req->count; i++) {
        void *subject = nullptr;
        std::size_t subject_len = 0;
        void *predicate = nullptr;
        std::size_t predicate_len = 0;

        int status = DATASTORE_ERROR; // only successful decoding sets the status to DATASTORE_SUCCESS
        if ((encode(req->subjects[i],   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (encode(req->predicates[i], &predicate, &predicate_len) == DATASTORE_SUCCESS)) {
            // create the key from the subject and predicate
            std::string key;
            sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                      ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                      key);

            std::string value;
            ::rocksdb::Status s = db->Get(::rocksdb::ReadOptions(), key, &value);

            if (s.ok()) {
                mlog(ROCKSDB_INFO, "Rank %d Rocksdb GET success", rank);

                // decode the object
                void *object = nullptr;
                std::size_t object_len = 0;
                if (decode(ReferenceBlob((void *) value.data(), value.size(), req->object_types[i]),
                           &object, &object_len) == DATASTORE_SUCCESS) {
                    res->objects[i] = RealBlob(object, object_len, req->object_types[i]);
                    event.size += res->objects[i].size();
                    status = DATASTORE_SUCCESS;
                    mlog(ROCKSDB_INFO, "Rank %d Rocksdb GET decode success", rank);
                }
            }
        }

        res->statuses[i] = status;

        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size(), req->subjects[i].data_type());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size(), req->predicates[i].data_type());

        if (subject != req->subjects[i].data()) {
            dealloc(subject);
        }
        if (predicate != req->predicates[i].data()) {
            dealloc(predicate);
        }
    }

    res->count = req->count;

    event.time.end = ::Stats::now();
    stats.gets.emplace_back(event);

    mlog(ROCKSDB_INFO, "Rank %d Rocksdb GET done processing %p", rank, req);
    return res;
}

/**
 * BGetOp
 * Performs a GetOp in rocksdb
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return pointer to a list of results
 */
Transport::Response::BGetOp *datastore::rocksdb::BGetOpImpl(Transport::Request::BGetOp *req) {
    Transport::Response::BGetOp *res = construct<Transport::Response::BGetOp>(req->count);

    ::rocksdb::Iterator *it = db->NewIterator(::rocksdb::ReadOptions());
    for(std::size_t i = 0; i < req->count; i++) {
        datastore::Datastore::Stats::Event event;
        event.time.start = ::Stats::now();

        // prepare response
        res->num_recs[i]     = 0;
        res->subjects[i]     = alloc_array<Blob>(req->num_recs[i]);
        res->predicates[i]   = alloc_array<Blob>(req->num_recs[i]);
        res->objects[i]      = alloc_array<Blob>(req->num_recs[i]);

        std::string key;
        if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_EQ) {
            sp_to_key(req->subjects[i], req->predicates[i], key);
            it->Seek(key);

            if (it->Valid()) {
                // set status early so failures during copy will change the status
                // all responses for this Op share a status
                res->statuses[i] = DATASTORE_UNSET;

                // only 1 response, so j == 0 (num_recs is ignored)
                this->template BGetOp_copy_response(it->key(), it->value(), req, res, i, 0, event);
            }
            else {
                BGetOp_error_response(res, i, req->subjects[i], req->predicates[i], event);

                res->statuses[i] = DATASTORE_ERROR;
            }
        }
        else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_NEXT) {
            sp_to_key(req->subjects[i], req->predicates[i], key);
            it->Seek(key);

            if (it->Valid()) {
                // set status early so failures during copy will change the status
                // all responses for this Op share a status
                res->statuses[i] = DATASTORE_UNSET;

                // first result returned is (subject, predicate)
                // (results are offsets)
                for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                    this->template BGetOp_copy_response(it->key(), it->value(), req, res, i, j, event);
                    it->Next();
                }
            }
            else {
                BGetOp_error_response(res, i, req->subjects[i], req->predicates[i], event);

                res->statuses[i] = DATASTORE_ERROR;
            }
        }
        else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_PREV) {
            sp_to_key(req->subjects[i], req->predicates[i], key);
            it->Seek(key);

            if (it->Valid()) {
                // set status early so failures during copy will change the status
                // all responses for this Op share a status
                res->statuses[i] = DATASTORE_UNSET;

                // first result returned is (subject, predicate)
                // (results are offsets)
                for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                    this->template BGetOp_copy_response(it->key(), it->value(), req, res, i, j, event);
                    it->Prev();
                }
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
                // set status early so failures during copy will change the status
                // all responses for this Op share a status
                res->statuses[i] = DATASTORE_UNSET;

                // first result returned is (subject, predicate)
                // (results are offsets)
                for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                    this->template BGetOp_copy_response(it->key(), it->value(), req, res, i, j, event);
                    it->Next();
                }
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
                // set status early so failures during copy will change the status
                // all responses for this Op share a status
                res->statuses[i] = DATASTORE_UNSET;

                // first result returned is (subject, predicate)
                // (results are offsets)
                for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                    this->template BGetOp_copy_response(it->key(), it->value(), req, res, i, j, event);
                    it->Prev();
                }
            }
            else {
                BGetOp_error_response(res, i, req->subjects[i], req->predicates[i], event);

                res->statuses[i] = DATASTORE_ERROR;
            }
        }

        if (res->statuses[i] == DATASTORE_UNSET) {
            res->statuses[i] = DATASTORE_SUCCESS;
        }

        res->count++;

        event.count = res->num_recs[i];
        event.time.end = ::Stats::now();
        stats.getops.emplace_back(event);
    }

    delete it;

    return res;
}

/**
 * BDelete
 * Performs a bulk DELETE in rocksdb
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @return pointer to a list of results
 */
Transport::Response::BDelete *datastore::rocksdb::BDeleteImpl(Transport::Request::BDelete *req) {
    datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BDelete *res = construct<Transport::Response::BDelete>(req->count);

    ::rocksdb::WriteBatch batch;

    // batch delete
    for(std::size_t i = 0; i < req->count; i++) {
        void *subject = nullptr;
        std::size_t subject_len = 0;
        void *predicate = nullptr;
        std::size_t predicate_len = 0;

        if ((encode(req->subjects[i],   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (encode(req->predicates[i], &predicate, &predicate_len) == DATASTORE_SUCCESS)) {
            // create the key from the subject and predicate
            std::string key;
            sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                      ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                      key);
            batch.Delete(key);
            event.size += key.size();
        }

        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size(), req->subjects[i].data_type());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size(), req->predicates[i].data_type());

        if (subject != req->subjects[i].data()) {
            dealloc(subject);
        }
        if (predicate != req->predicates[i].data()) {
            dealloc(predicate);
        }
    }

    // create responses
    ::rocksdb::Status status = db->Write(::rocksdb::WriteOptions(), &batch);

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
int datastore::rocksdb::SyncImpl() {
    ::rocksdb::WriteBatch batch;
    ::rocksdb::WriteOptions options;
    options.sync = true;
    return db->Write(options, &batch).ok()?DATASTORE_SUCCESS:DATASTORE_ERROR;
}
