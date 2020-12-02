#include <deque>
#include <sstream>

#include "leveldb/write_batch.h"

#include "datastore/leveldb.hpp"
#include "hxhim/Blob.hpp"
#include "hxhim/accessors.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

datastore::leveldb::leveldb(const int rank,
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

datastore::leveldb::~leveldb() {
    Close();
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
        return false;
    }
    mlog(LEVELDB_INFO, "Opened leveldb with name: %s", name.c_str());
    #if PRINT_TIMESTAMPS
    ::Stats::print_event(std::cerr, rank, "leveldb_open", ::Stats::global_epoch, leveldb_open);
    #endif

    return status.ok();
}

void datastore::leveldb::CloseImpl() {
    delete db;
    db = nullptr;
}

const std::string &datastore::leveldb::Name() const {
    return name;
}

/**
 * BPut
 * Performs a bulk PUT in leveldb
 *
 * @param req  the packet requesting multiple PUTs
 * @return pointer to a list of results
 */
Transport::Response::BPut *datastore::leveldb::BPutImpl(Transport::Request::BPut *req) {
    mlog(LEVELDB_INFO, "LevelDB BPut");

    datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BPut *res = construct<Transport::Response::BPut>(req->count);

    // batch up PUTs
    ::leveldb::WriteBatch batch;
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

            batch.Put(::leveldb::Slice(key.c_str(), key.size()), ::leveldb::Slice((char *) object, object_len));

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

    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);

    if (status.ok()) {
        // successful writes only update statuses that were unset
        for(std::size_t i = 0; i < req->count; i++) {
            if (res->statuses[i] == DATASTORE_UNSET) {
                res->statuses[i] = DATASTORE_SUCCESS;
            }
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
 * @param req  the packet requesting multiple GETs
 * @return pointer to a list of results
 */
Transport::Response::BGet *datastore::leveldb::BGetImpl(Transport::Request::BGet *req) {
    mlog(LEVELDB_INFO, "Rank %d LevelDB GET processing %zu item in %ps", rank, req->count, req);

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
        if ((encode(callbacks, req->subjects[i],   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (encode(callbacks, req->predicates[i], &predicate, &predicate_len) == DATASTORE_SUCCESS)) {
            // create the key from the subject and predicate
            std::string key;
            sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                      ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                      key);

            std::string value;
            ::leveldb::Status s = db->Get(::leveldb::ReadOptions(), key, &value);

            if (s.ok()) {
                mlog(LEVELDB_INFO, "Rank %d LevelDB GET success", rank);

                // decode the object
                void *object = nullptr;
                std::size_t object_len = 0;
                if (decode(callbacks, ReferenceBlob((void *) value.data(), value.size(), req->object_types[i]),
                           &object, &object_len) == DATASTORE_SUCCESS) {
                    res->objects[i] = RealBlob(object, object_len, req->object_types[i]);
                    event.size += res->objects[i].size();
                    status = DATASTORE_SUCCESS;
                    mlog(LEVELDB_INFO, "Rank %d LevelDB GET decode success", rank);
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

    mlog(LEVELDB_INFO, "Rank %d LevelDB GET done processing %p", rank, req);
    return res;
}

/**
 * BGetOp
 * Performs a GetOp in leveldb
 *
 * @param req  the packet requesting multiple GETOPs
 * @return pointer to a list of results
 */
Transport::Response::BGetOp *datastore::leveldb::BGetOpImpl(Transport::Request::BGetOp *req) {
    Transport::Response::BGetOp *res = construct<Transport::Response::BGetOp>(req->count);

    ::leveldb::Iterator *it = db->NewIterator(::leveldb::ReadOptions());
    for(std::size_t i = 0; i < req->count; i++) {
        datastore::Datastore::Stats::Event event;
        event.time.start = ::Stats::now();

        // prepare response
        res->num_recs[i]   = 0;
        res->subjects[i]   = alloc_array<Blob>(req->num_recs[i]);
        res->predicates[i] = alloc_array<Blob>(req->num_recs[i]);
        res->objects[i]    = alloc_array<Blob>(req->num_recs[i]);
        // set status early so failures during copy will change the status
        // all responses for this Op share a status
        res->statuses[i]   = DATASTORE_UNSET;

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

        res->count++;

        event.count = res->num_recs[i];
        event.time.end = ::Stats::now();
        stats.gets.emplace_back(event);
    }

    delete it;

    return res;
}

/**
 * BDelete
 * Performs a bulk DELETE in leveldb
 *
 * @param req  the packet requesting multiple DELETEs
 * @return pointer to a list of results
 */
Transport::Response::BDelete *datastore::leveldb::BDeleteImpl(Transport::Request::BDelete *req) {
    datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BDelete *res = construct<Transport::Response::BDelete>(req->count);

    ::leveldb::WriteBatch batch;

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
    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);

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
 * WriteHistogramsImpl
 * Writes all histograms to the underlying datastore.
 *
 * @return DATASTORE_SUCCESS
 */
int datastore::leveldb::WriteHistogramsImpl() {
    ::leveldb::WriteBatch batch;

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

        batch.Put(::leveldb::Slice(key.c_str(), key.size()),
                  ::leveldb::Slice((char *) serial_hist, serial_hist_len));
    }

    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);

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
std::size_t datastore::leveldb::ReadHistogramsImpl(const datastore::HistNames_t &names) {
    std::size_t found = 0;

    for(std::string const &name : names) {
        // Create the key from the fixed subject and the histogram name
        std::string key;
        sp_to_key(ReferenceBlob((char *) HISTOGRAM_SUBJECT.data(), HISTOGRAM_SUBJECT.size(), hxhim_data_t::HXHIM_DATA_BYTE),
                  ReferenceBlob((char *) name.data(), name.size(), hxhim_data_t::HXHIM_DATA_BYTE),
                  key);

        // Search for the histogram
        std::string serial_hist;
        ::leveldb::Status status = db->Get(::leveldb::ReadOptions(), key, &serial_hist);

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
int datastore::leveldb::SyncImpl() {
    ::leveldb::WriteBatch batch;
    ::leveldb::WriteOptions options;
    options.sync = true;
    return db->Write(options, &batch).ok()?DATASTORE_SUCCESS:DATASTORE_ERROR;
}
