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
                              Transform::Callbacks *callbacks,
                              Histogram::Histogram *hist,
                              const std::string &basename)
    : Datastore(rank, id, callbacks, hist),
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
            if (sp_to_key(ReferenceBlob(subject,   subject_len,   req->subjects[i].data_type()),
                          ReferenceBlob(predicate, predicate_len, req->predicates[i].data_type()),
                          key) == HXHIM_SUCCESS) {
                decltype(db)::const_iterator it = db.find(key);
                if (it != db.end()) {
                    // decode the object
                    void *object = nullptr;
                    std::size_t object_len = 0;
                    if (decode(callbacks, ReferenceBlob((void *) it->second.data(), it->second.size(), req->object_types[i]),
                               &object, &object_len) == DATASTORE_SUCCESS) {

                        res->objects[i] = RealBlob(object, object_len, req->object_types[i]);
                        event.size += res->objects[i].size();
                        status = DATASTORE_SUCCESS;
                    }
                }

                event.size += key.size();
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
Transport::Response::BGetOp *datastore::InMemory::BGetOpImpl(Transport::Request::BGetOp *req) {
    Transport::Response::BGetOp *res = construct<Transport::Response::BGetOp>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        datastore::Datastore::Stats::Event event;
        event.time.start = ::Stats::now();

        decltype(db)::const_iterator it = db.end();

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
Transport::Response::BDelete *datastore::InMemory::BDeleteImpl(Transport::Request::BDelete *req) {
    datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BDelete *res = construct<Transport::Response::BDelete>(req->count);

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
