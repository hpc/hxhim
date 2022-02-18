#include <deque>
#include <sstream>

#include "rocksdb/write_batch.h"

#include "datastore/RocksDB.hpp"
#include "datastore/triplestore.hpp"
#include "hxhim/accessors.hpp"
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
    mlog(ROCKSDB_INFO, "Opened rocksdb with name: %s", name.c_str());
    #if PRINT_TIMESTAMPS
    ::Stats::print_event(std::cerr, rank, "rocksdb_open", ::Stats::global_epoch, rocksdb_open);
    #endif

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
    mlog(ROCKSDB_INFO, "RocksDB BPut");

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

        int status = DATASTORE_ERROR; // successful batching will set to DATASTORE_UNSET
        if ((encode(callbacks, req->subjects[i],   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (encode(callbacks, req->predicates[i], &predicate, &predicate_len) == DATASTORE_SUCCESS) &&
            (encode(callbacks, req->objects[i],    &object,    &object_len)    == DATASTORE_SUCCESS)) {
            // the current key address and length
            Blob key;
            sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                      ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                      &key);

            Blob value = append_type(object, object_len, req->objects[i].data_type());

            batch.Put(::rocksdb::Slice((char *) key.data(), key.size()),
                      ::rocksdb::Slice((char *) value.data(), value.size()));

            event.size += key.size() + value.size();
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
    }

    ::rocksdb::Status status = db->Write(::rocksdb::WriteOptions(), &batch);
    const int res_status = status.ok()?DATASTORE_SUCCESS:DATASTORE_ERROR;

    for(std::size_t i = 0; i < req->count; i++) {
        res->add(ReferenceBlob(req->orig.subjects[i], req->subjects[i].size(), req->subjects[i].data_type()),
                 ReferenceBlob(req->orig.predicates[i], req->predicates[i].size(), req->predicates[i].data_type()),
                 res_status);
    }

    event.time.end = ::Stats::now();
    stats.puts.emplace_back(event);

    mlog(ROCKSDB_INFO, "RocksDB BPut Completed");
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
    mlog(ROCKSDB_INFO, "Rank %d RocksDB GET processing %zu item in %ps", rank, req->count, req);

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
        hxhim_data_t object_type = req->object_types[i];

        int status = DATASTORE_ERROR; // only successful decoding sets the status to DATASTORE_SUCCESS
        if ((encode(callbacks, req->subjects[i],   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (encode(callbacks, req->predicates[i], &predicate, &predicate_len) == DATASTORE_SUCCESS)) {
            // create the key from the subject and predicate
            Blob key;
            sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                      ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                      &key);

            std::string value;
            ::rocksdb::Status s = db->Get(::rocksdb::ReadOptions(),
                                          ::rocksdb::Slice((char *) key.data(), key.size()),
                                          &value);

            if (s.ok()) {
                mlog(ROCKSDB_INFO, "Rank %d RocksDB GET success", rank);

                std::size_t value_len = value.size();
                object_type = remove_type((void *) value.data(), value_len);
                if (object_type != req->object_types[i]) {
                    mlog(DATASTORE_WARN, "GET extracted object data type (%s) does not match provided data type (%s). Using extracted type.",
                         HXHIM_DATA_STR[object_type],
                         HXHIM_DATA_STR[req->object_types[i]]);
                }

                // decode the object
                if (decode(callbacks, Blob((void *) value.data(), value_len, object_type),
                           &object, &object_len) == DATASTORE_SUCCESS) {
                    event.size += res->objects[i].size();
                    status = DATASTORE_SUCCESS;
                    mlog(ROCKSDB_INFO, "Rank %d RocksDB GET decode success", rank);
                }
            }
        }

        // object will be owned by the result packet
        res->add(ReferenceBlob(req->orig.subjects[i], req->subjects[i].size(), req->subjects[i].data_type()),
                 ReferenceBlob(req->orig.predicates[i], req->predicates[i].size(), req->predicates[i].data_type()),
                 RealBlob(object, object_len, object_type),
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

    mlog(ROCKSDB_INFO, "Rank %d RocksDB GET done processing %p", rank, req);
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

        void *subject = nullptr;
        std::size_t subject_len = 0;
        void *predicate = nullptr;
        std::size_t predicate_len = 0;
        Blob key;

        BGetOp_loop_init(req, res, i, subject, subject_len, predicate, predicate_len, key);

        if (res->statuses[i] == DATASTORE_UNSET) {
            if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_EQ) {
                it->Seek(::rocksdb::Slice((char *) key.data(), key.size()));

                if (it->Valid()) {
                    // only 1 response, so j == 0 (num_recs is ignored)
                    this->template BGetOp_copy_response(callbacks,
                                                        it->key(), it->value(),
                                                        req, res, i, 0, event);
                }
                else {
                    res->statuses[i] = DATASTORE_ERROR;
                }
            }
            else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_NEXT) {
                it->Seek(::rocksdb::Slice((char *) key.data(), key.size()));

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
                it->Seek(::rocksdb::Slice((char *) key.data(), key.size()));

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
            else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_LOWEST) {
                const std::size_t prefix_len = subject_len + predicate_len;
                const std::string seek = BGetOp_get_seek(key, prefix_len, predicate_len,
                                                         req->predicates[i].data_type());
                ::rocksdb::Slice prefix((char *) key.data(), prefix_len); // need prefix for starts_with()

                it->Seek(seek);

                if (it->Valid()) {
                    for(std::size_t j = 0;
                        (j < req->num_recs[i]) &&
                        it->Valid() &&
                        it->key().starts_with(prefix);
                        j++) {
                        this->template BGetOp_copy_response(callbacks, it->key(), it->value(), req, res, i, j, event);
                        it->Next();
                    }
                }
                else {
                    res->statuses[i] = DATASTORE_ERROR;
                }
            }
            else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_HIGHEST) {
                const std::size_t prefix_len = subject_len + predicate_len;
                const std::string seek = BGetOp_get_seek(key, prefix_len, predicate_len,
                                                         req->predicates[i].data_type());
                ::rocksdb::Slice prefix((char *) key.data(), prefix_len); // need prefix for starts_with()

                it->Seek(::rocksdb::Slice(seek.data(), seek.size()));

                // go back one since the seek value is guaranteed to be past the last value found
                if (it->Valid()) {
                    it->Prev();
                }
                else {
                    it->SeekToLast();
                }

                if (it->Valid()) {
                    // walk backwards to get values
                    for(std::size_t j = 0;
                        (j < req->num_recs[i]) &&
                        it->Valid() &&
                        it->key().starts_with(prefix);
                        j++) {
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

        res->update_size(i);

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
            Blob key;
            sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                      ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                      &key);
            batch.Delete(::rocksdb::Slice((char *) key.data(), key.size()));
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
        Blob key;
        sp_to_key(Blob(HISTOGRAM_SUBJECT), Blob(hist.first), &key);

        void *serial_hist = nullptr;
        std::size_t serial_hist_len = 0;

        hist.second->pack(&serial_hist, &serial_hist_len);
        ptrs.push_back(serial_hist);

        batch.Put(::rocksdb::Slice((char *) key.data(), key.size()),
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
        Blob key;
        sp_to_key(Blob(HISTOGRAM_SUBJECT), Blob(name), &key);

        // Search for the histogram
        std::string serial_hist;
        ::rocksdb::Status status = db->Get(::rocksdb::ReadOptions(),
                                           ::rocksdb::Slice((char *) key.data(), key.size()),
                                           &serial_hist);

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
