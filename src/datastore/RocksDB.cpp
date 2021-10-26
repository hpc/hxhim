#include <deque>
#include <sstream>

#include "rocksdb/write_batch.h"

#include "datastore/RocksDB.hpp"
#include "hxhim/accessors.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

Datastore::RocksDB::RocksDB(const int rank,
                            const int id,
                            Transform::Callbacks *callbacks,
                            const bool create_if_missing)
    : Datastore(rank, id, callbacks),
      name(),
      create_if_missing(create_if_missing),
      db(nullptr), options()
{
    options.create_if_missing = create_if_missing;
}

Datastore::RocksDB::~RocksDB() {
    Close();
}

bool Datastore::RocksDB::OpenImpl(const std::string &new_name) {
    #if PRINT_TIMESTAMPS
    ::Stats::Chronostamp rocksdb_open;
    rocksdb_open.start = ::Stats::now();
    #endif
    name = new_name;
    ::rocksdb::Status status = ::rocksdb::DB::Open(options, name, &db);
    #if PRINT_TIMESTAMPS
    rocksdb_open.end = ::Stats::now();
    #endif
    if (!status.ok()) {
        return false;
    }
    #if PRINT_TIMESTAMPS
    ::Stats::print_event(std::cerr, rank, "rocksdb_open", ::Stats::global_epoch, rocksdb_open);
    #endif
    mlog(ROCKSDB_INFO, "Opened rocksdb with name: %s", name.c_str());
    return status.ok();
}

void Datastore::RocksDB::CloseImpl() {
    delete db;
    db = nullptr;
}

bool Datastore::RocksDB::UsableImpl() const {
    return db;
}

const std::string &Datastore::RocksDB::Name() const {
    return name;
}

/**
 * BPut
 * Performs a bulk PUT in rocksdb
 *
 * @param req  the packet requesting multiple PUTs
 * @return pointer to a list of results
 */
Message::Response::BPut *Datastore::RocksDB::BPutImpl(Message::Request::BPut *req) {
    mlog(ROCKSDB_INFO, "Rocksdb BPut");

    Datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Message::Response::BPut *res = construct<Message::Response::BPut>(req->count);

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
        if ((encode(callbacks, req->subjects[i],   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (encode(callbacks, req->predicates[i], &predicate, &predicate_len) == DATASTORE_SUCCESS) &&
            (encode(callbacks, req->objects[i],    &object,    &object_len)    == DATASTORE_SUCCESS)) {
            // the current key address and length
            std::string key;
            sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                      ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                      key);

            batch.Put(::rocksdb::Slice(key.c_str(), key.size()), ::rocksdb::Slice((char *) object, object_len));

            event.size += key.size() + object_len;
            status = DATASTORE_UNSET;
        }

        if (subject != req->subjects[i].data()) {
            dealloc(subject);
        }
        if (predicate != req->predicates[i].data()) {
            dealloc(predicate);
        }
        if (object != req->objects[i].data()) {
            dealloc(object);
        }

        res->add(ReferenceBlob(req->orig.subjects[i], req->subjects[i].size(), req->subjects[i].data_type()),
                 ReferenceBlob(req->orig.predicates[i], req->predicates[i].size(), req->predicates[i].data_type()),
                 status);
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

    event.time.end = ::Stats::now();
    stats.puts.emplace_back(event);

    mlog(ROCKSDB_INFO, "Rocksdb BPut Completed");
    return res;
}

/**
 * BGet
 * Performs a bulk GET in rocksdb
 *
 * @param req  the packet requesting multiple GETs
 * @return pointer to a list of results
 */
Message::Response::BGet *Datastore::RocksDB::BGetImpl(Message::Request::BGet *req) {
    mlog(ROCKSDB_INFO, "Rank %d Rocksdb GET processing %zu item in %ps", rank, req->count, req);

    Datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Message::Response::BGet *res = construct<Message::Response::BGet>(req->count);

    // batch up GETs
    for(std::size_t i = 0; i < req->count; i++) {
        void *subject = nullptr;
        std::size_t subject_len = 0;
        void *predicate = nullptr;
        std::size_t predicate_len = 0;
        void *object = nullptr;
        std::size_t object_len = 0;
        int status = DATASTORE_ERROR; // only successful decoding sets the status to DATASTORE_SUCCESS
        if ((encode(callbacks, req->subjects[i],   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (encode(callbacks, req->predicates[i], &predicate, &predicate_len) == DATASTORE_SUCCESS)) {
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
                if (decode(callbacks, ReferenceBlob((void *) value.data(), value.size(), req->object_types[i]),
                           &object, &object_len) == DATASTORE_SUCCESS) {
                    event.size += res->objects[i].size();
                    status = DATASTORE_SUCCESS;
                    mlog(ROCKSDB_INFO, "Rank %d Rocksdb GET decode success", rank);
                }
            }
        }

        res->add(ReferenceBlob(req->orig.subjects[i], req->subjects[i].size(), req->subjects[i].data_type()),
                 ReferenceBlob(req->orig.predicates[i], req->predicates[i].size(), req->predicates[i].data_type()),
                 RealBlob(object, object_len, req->object_types[i]),
                 status);

        if (subject != req->subjects[i].data()) {
            dealloc(subject);
        }
        if (predicate != req->predicates[i].data()) {
            dealloc(predicate);
        }
    }

    event.time.end = ::Stats::now();
    stats.gets.emplace_back(event);

    mlog(ROCKSDB_INFO, "Rank %d Rocksdb GET done processing %p", rank, req);
    return res;
}

/**
 * BGetOp
 * Performs a GetOp in rocksdb
 *
 * @param req  the packet requesting multiple GETOPs
 * @return pointer to a list of results
 */
Message::Response::BGetOp *Datastore::RocksDB::BGetOpImpl(Message::Request::BGetOp *req) {
    Message::Response::BGetOp *res = construct<Message::Response::BGetOp>(req->count);

    ::rocksdb::Iterator *it = db->NewIterator(::rocksdb::ReadOptions());
    for(std::size_t i = 0; i < req->count; i++) {
        Datastore::Datastore::Stats::Event event;
        event.time.start = ::Stats::now();

        // prepare response
        // does not modify serialized size
        // set status early so failures during copy will change the status
        // all responses for this Op share a status
        res->add(alloc_array<Blob>(req->num_recs[i]),
                 alloc_array<Blob>(req->num_recs[i]),
                 alloc_array<Blob>(req->num_recs[i]),
                 0,
                 DATASTORE_UNSET);

        // encode the subject and predicate and get the key
        void *subject = nullptr;
        std::size_t subject_len = 0;
        void *predicate = nullptr;
        std::size_t predicate_len = 0;
        std::string key;
        if ((req->ops[i] == hxhim_getop_t::HXHIM_GETOP_EQ)   ||
            (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_NEXT) ||
            (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_PREV)) {
            if ((encode(callbacks, req->subjects[i],   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
                (encode(callbacks, req->predicates[i], &predicate, &predicate_len) == DATASTORE_SUCCESS)) {
                if (sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                              ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                              key) != HXHIM_SUCCESS) {
                    res->statuses[i] = DATASTORE_ERROR;
                }
            }
            else {
                res->statuses[i] = DATASTORE_ERROR;
            }
        }

        if (res->statuses[i] == DATASTORE_UNSET) {
            if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_EQ) {
                it->Seek(key);

                if (it->Valid()) {
                    // only 1 response, so j == 0 (num_recs is ignored)
                    this->template BGetOp_copy_response(callbacks, it->key(), it->value(), req, res, i, 0, event);
                }
                else {
                    res->statuses[i] = DATASTORE_ERROR;
                }
            }
            else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_NEXT) {
                it->Seek(key);

                if (it->Valid()) {
                    // first result returned is (subject, predicate)
                    // (results are offsets)
                    for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                        this->template BGetOp_copy_response(callbacks, it->key(), it->value(), req, res, i, j, event);
                        it->Next();
                    }
                }
                else {
                    res->statuses[i] = DATASTORE_ERROR;
                }
            }
            else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_PREV) {
                it->Seek(key);

                if (it->Valid()) {
                    // first result returned is (subject, predicate)
                    // (results are offsets)
                    for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                        this->template BGetOp_copy_response(callbacks, it->key(), it->value(), req, res, i, j, event);
                        it->Prev();
                    }
                }
                else {
                    res->statuses[i] = DATASTORE_ERROR;
                }
            }
            else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_FIRST) {
                // ignore key
                it->SeekToFirst();

                if (it->Valid()) {
                    for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                        this->template BGetOp_copy_response(callbacks, it->key(), it->value(), req, res, i, j, event);
                        it->Next();
                    }
                }
                else {
                    res->statuses[i] = DATASTORE_ERROR;
                }
            }
            else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_LAST) {
                // ignore key
                it->SeekToLast();

                if (it->Valid()) {
                    for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                        this->template BGetOp_copy_response(callbacks, it->key(), it->value(), req, res, i, j, event);
                        it->Prev();
                    }
                }
                else {
                    res->statuses[i] = DATASTORE_ERROR;
                }
            }
            else {
                res->statuses[i] = DATASTORE_ERROR;
            }
        }

        if (subject != req->subjects[i].data()) {
            dealloc(subject);
        }
        if (predicate != req->predicates[i].data()) {
            dealloc(predicate);
        }

        // if the status is still DATASTORE_UNSET, the operation succeeded
        if (res->statuses[i] == DATASTORE_UNSET) {
            res->statuses[i] = DATASTORE_SUCCESS;
        }
        else /* if (res->statuses[i] == DATASTORE_ERROR) */ {
            BGetOp_error_response(res, i, req->subjects[i], req->predicates[i], event);
        }

        event.count = res->num_recs[i];
        event.time.end = ::Stats::now();
        stats.gets.emplace_back(event);
    }

    delete it;

    return res;
}

/**
 * BDelete
 * Performs a bulk DELETE in rocksdb
 *
 * @param req  the packet requesting multiple DELETEs
 * @return pointer to a list of results
 */
Message::Response::BDelete *Datastore::RocksDB::BDeleteImpl(Message::Request::BDelete *req) {
    Datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Message::Response::BDelete *res = construct<Message::Response::BDelete>(req->count);

    ::rocksdb::WriteBatch batch;

    // batch delete
    for(std::size_t i = 0; i < req->count; i++) {
        void *subject = nullptr;
        std::size_t subject_len = 0;
        void *predicate = nullptr;
        std::size_t predicate_len = 0;

        if ((encode(callbacks, req->subjects[i],   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (encode(callbacks, req->predicates[i], &predicate, &predicate_len) == DATASTORE_SUCCESS)) {
            // create the key from the subject and predicate
            std::string key;
            sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                      ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                      key);
            batch.Delete(key);
            event.size += key.size();
        }

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
        res->add(ReferenceBlob(req->orig.subjects[i], req->subjects[i].size(), req->subjects[i].data_type()),
                 ReferenceBlob(req->orig.predicates[i], req->predicates[i].size(), req->predicates[i].data_type()),
                 stat);
    }

    event.time.end = ::Stats::now();
    stats.deletes.emplace_back(event);

    return res;
}

/**
 * WriteHistogramsImpl
 * Writes all histograms to the underlying datastore.
 *
 * @return DATASTORE_SUCCESS
 */
int Datastore::RocksDB::WriteHistogramsImpl() {
    ::rocksdb::WriteBatch batch;

    std::deque<void *> ptrs;
    for(decltype(hists)::value_type hist : hists) {
        std::string key;
        sp_to_key(ReferenceBlob((char *) HISTOGRAM_SUBJECT.data(), HISTOGRAM_SUBJECT.size(), hxhim_data_t::HXHIM_DATA_BYTE),
                  ReferenceBlob((char *) hist.first.data(), hist.first.size(), hxhim_data_t::HXHIM_DATA_BYTE),
                  key);

        void *serial_hist = nullptr;
        std::size_t serial_hist_len = 0;

        hist.second->pack(&serial_hist, &serial_hist_len);
        ptrs.push_back(serial_hist);

        batch.Put(::rocksdb::Slice(key.c_str(), key.size()),
                  ::rocksdb::Slice((char *) serial_hist, serial_hist_len));
    }

    ::rocksdb::Status status = db->Write(::rocksdb::WriteOptions(), &batch);

    for(void *ptr : ptrs) {
        dealloc(ptr);
    }

    return status.ok()?DATASTORE_SUCCESS:DATASTORE_ERROR;
}

/**
 * ReadHistogramsImpl
 * Searches for histograms in the underlying datastore
 * that have names from the provided list. Histogram
 * instances that exist are overwritten.
 *
 * @param names  A list of histogram names to look for
 * @return The number of histograms found
 */
std::size_t Datastore::RocksDB::ReadHistogramsImpl(const HistNames_t &names) {
    std::size_t found = 0;

    for(std::string const &name : names) {
        // Create the key from the fixed subject and the histogram name
        std::string key;
        sp_to_key(ReferenceBlob((char *) HISTOGRAM_SUBJECT.data(), HISTOGRAM_SUBJECT.size(), hxhim_data_t::HXHIM_DATA_BYTE),
                  ReferenceBlob((char *) name.data(), name.size(), hxhim_data_t::HXHIM_DATA_BYTE),
                  key);

        // Search for the histogram
        std::string serial_hist;
        ::rocksdb::Status status = db->Get(::rocksdb::ReadOptions(), key, &serial_hist);

        if (status.ok()) {
            std::shared_ptr<::Histogram::Histogram> new_hist(construct<::Histogram::Histogram>(),
                                                             ::Histogram::deleter);

            // parse serialized data
            if(new_hist->unpack((void *) serial_hist.data(), serial_hist.size())) {
                // overwrite existing
                hists[name] = new_hist;

                found++;
            }
        }
    }

    return found;
}

/**
 * Sync
 * Syncs the database to disc
 *
 * @return DATASTORE_SUCCESS or DATASTORE_ERROR on error
 */
int Datastore::RocksDB::SyncImpl() {
    ::rocksdb::WriteBatch batch;
    ::rocksdb::WriteOptions options;
    options.sync = true;
    return db->Write(options, &batch).ok()?DATASTORE_SUCCESS:DATASTORE_ERROR;
}
