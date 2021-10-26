#include <deque>
#include <sstream>

#include "datastore/InMemory.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"

Datastore::InMemory::InMemory(const int rank,
                              const int id,
                              Transform::Callbacks *callbacks)
    : Datastore(rank, id, callbacks),
      good(false),
      db()
{}

Datastore::InMemory::~InMemory() {
    Close();
}

bool Datastore::InMemory::Open() {
    return OpenImpl("");
}

bool Datastore::InMemory::OpenImpl(const std::string &) {
    db.clear();
    good = true;
    return Usable();
}

void Datastore::InMemory::CloseImpl() {
    good = false;
    db.clear();
}

bool Datastore::InMemory::UsableImpl() const {
    return good;
}

/**
 * BPut
 * Performs a bulk PUT in
 *
 * @param req  the packet requesting multiple PUTs
 * @return pointer to a list of results
 */
Message::Response::BPut *Datastore::InMemory::BPutImpl(Message::Request::BPut *req) {
    Datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Message::Response::BPut *res = construct<Message::Response::BPut>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        void *subject = nullptr;
        std::size_t subject_len = 0;
        void *predicate = nullptr;
        std::size_t predicate_len = 0;
        void *object = nullptr;
        std::size_t object_len = 0;

        int status = DATASTORE_ERROR; // only successful writes sets the status to DATASTORE_SUCCESS
        if ((encode(callbacks, req->subjects[i],   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (encode(callbacks, req->predicates[i], &predicate, &predicate_len) == DATASTORE_SUCCESS) &&
            (encode(callbacks, req->objects[i],    &object,    &object_len)    == DATASTORE_SUCCESS)) {
            // the current key address and length
            std::string key;
            if (sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                          ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                          key) == HXHIM_SUCCESS) {
                db[key] = std::string((char *) object, object_len);

                event.size += key.size() + object_len;
                status = DATASTORE_SUCCESS;
            }
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

    event.time.end = ::Stats::now();
    stats.puts.emplace_back(event);

    return res;
}

/**
 * BGet
 * Performs a bulk GET in InMemory
 *
 * @param req  the packet requesting multiple GETs
 * @return pointer to a list of results
 */
Message::Response::BGet *Datastore::InMemory::BGetImpl(Message::Request::BGet *req) {
    Datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Message::Response::BGet *res = construct<Message::Response::BGet>(req->count);

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
            if (sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                          ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                          key) == HXHIM_SUCCESS) {
                decltype(db)::const_iterator it = db.find(key);
                if (it != db.end()) {
                    // decode the object
                    if (decode(callbacks, ReferenceBlob((void *) it->second.data(), it->second.size(), req->object_types[i]),
                               &object, &object_len) == DATASTORE_SUCCESS) {
                        event.size += res->objects[i].size();
                        status = DATASTORE_SUCCESS;
                    }
                }

                event.size += key.size();
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

    return res;
}

/**
 * BGetOp
 * Performs a GetOp in InMemory
 *
 * @param req  the packet requesting multiple GETOPs
 * @return pointer to a list of results
 */
Message::Response::BGetOp *Datastore::InMemory::BGetOpImpl(Message::Request::BGetOp *req) {
    Message::Response::BGetOp *res = construct<Message::Response::BGetOp>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        Datastore::Datastore::Stats::Event event;
        event.time.start = ::Stats::now();

        decltype(db)::const_iterator it = db.end();

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
                it = db.find(key);

                if (it != db.end()) {
                    // only 1 response, so j == 0 (num_recs is ignored)
                    this->template BGetOp_copy_response(callbacks, it->first, it->second, req, res, i, 0, event);
                }
                else {
                    res->statuses[i] = DATASTORE_ERROR;
                }
            }
            else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_NEXT) {
                it = db.find(key);

                if (it != db.end()) {
                    // first result returned is (subject, predicate)
                    // (results are offsets)
                    for(std::size_t j = 0; (j < req->num_recs[i]) && (it != db.end()); j++) {
                        this->template BGetOp_copy_response(callbacks, it->first, it->second, req, res, i, j, event);
                        it++;
                    }
                }
                else {
                    res->statuses[i] = DATASTORE_ERROR;
                }
            }
            else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_PREV) {
                it = db.find(key);

                if (it != db.end()) {
                    // first result returned is (subject, predicate)
                    // (results are offsets)
                    for(std::size_t j = 0; j < req->num_recs[i]; j++) {
                        this->template BGetOp_copy_response(callbacks, it->first, it->second, req, res, i, j, event);
                        if (it == db.begin()) {
                            break;
                        }
                        it--;
                    }
                }
                else {
                    res->statuses[i] = DATASTORE_ERROR;
                }
            }
            else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_FIRST) {
                // ignore key
                it = db.begin();

                if (it != db.end()) {
                    for(std::size_t j = 0; (j < req->num_recs[i]) && (it != db.end()); j++) {
                        this->template BGetOp_copy_response(callbacks, it->first, it->second, req, res, i, j, event);
                        it++;
                    }
                }
                else {
                    res->statuses[i] = DATASTORE_ERROR;
                }
            }
            else if (req->ops[i] == hxhim_getop_t::HXHIM_GETOP_LAST) {
                // ignore key
                it = db.end();
                it--;

                if (it != db.end()) {
                    for(std::size_t j = 0; j < req->num_recs[i]; j++) {
                        this->template BGetOp_copy_response(callbacks, it->first, it->second, req, res, i, j, event);
                        if (it == db.begin()) {
                            break;
                        }
                        it--;
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

    return res;
}

/**
 * BDelete
 * Performs a bulk DELETE in InMemory
 *
 * @param req  the packet requesting multiple DELETEs
 * @return pointer to a list of results
 */
Message::Response::BDelete *Datastore::InMemory::BDeleteImpl(Message::Request::BDelete *req) {
    Datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Message::Response::BDelete *res = construct<Message::Response::BDelete>(req->count);

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

            decltype(db)::const_iterator it = db.find(key);
            if (it != db.end()) {
                db.erase(it);
                status = DATASTORE_SUCCESS;
            }
            else {
                status = DATASTORE_ERROR;
            }

            event.size += key.size();
        }

        res->add(ReferenceBlob(req->orig.subjects[i], req->subjects[i].size(), req->subjects[i].data_type()),
                 ReferenceBlob(req->orig.predicates[i], req->predicates[i].size(), req->predicates[i].data_type()),
                 status);

        if (subject != req->subjects[i].data()) {
            dealloc(subject);
        }
        if (predicate != req->predicates[i].data()) {
            dealloc(predicate);
        }
    }

    res->count = req->count;

    event.time.end = ::Stats::now();
    stats.deletes.emplace_back(event);

    return res;
}

/**
 * WriteHistogramsImpl
 * Write histograms to the map.
 *
 * @return DATASTORE_SUCCESS
 */
int Datastore::InMemory::WriteHistogramsImpl() {
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

        db[key] = std::string((char *) serial_hist, serial_hist_len);
    }

    for(void *ptr : ptrs) {
        dealloc(ptr);
    }

    return DATASTORE_SUCCESS;
}

/**
 * ReadHistogramsImpl
 * Reads histograms found in the map. Histogram
 * instances that exist are overwritten.
 *
 * @param names  A list of histogram names to look for
 * @return The number of histograms found
 */
std::size_t Datastore::InMemory::ReadHistogramsImpl(const HistNames_t &names) {
    std::size_t found = 0;

    for(std::string const &name : names) {
        // Create the key from the fixed subject and the histogram name
        std::string key;
        sp_to_key(ReferenceBlob((char *) HISTOGRAM_SUBJECT.data(), HISTOGRAM_SUBJECT.size(), hxhim_data_t::HXHIM_DATA_BYTE),
                  ReferenceBlob((char *) name.data(), name.size(), hxhim_data_t::HXHIM_DATA_BYTE),
                  key);

        // Search for the histogram
        decltype(db)::const_iterator it = db.find(key);
        if (it != db.end()) {
            std::shared_ptr<::Histogram::Histogram> new_hist(construct<::Histogram::Histogram>(),
                                                             ::Histogram::deleter);

            // parse serialized data
            if(new_hist->unpack((void *) it->second.data(), it->second.size())) {
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
 * NOOP
 *
 * @return DATASTORE_SUCCESS
 */
int Datastore::InMemory::SyncImpl() {
    return DATASTORE_SUCCESS;
}
