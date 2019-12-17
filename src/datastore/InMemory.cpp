#include <ctime>
#include <sstream>
#include <stdexcept>

#include "datastore/InMemory.hpp"
#include "hxhim/private.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"

namespace hxhim {
namespace datastore {

using namespace Transport;

InMemory::InMemory(hxhim_t *hx,
                   Histogram::Histogram *hist,
                   const std::string &exact_name)
    : Datastore(hx, 0, hist),
      db()
{
    Datastore::Open(exact_name);
}

InMemory::InMemory(hxhim_t *hx,
                   const int id,
                   Histogram::Histogram *hist,
                   const std::string &basename)
    : Datastore(hx, id, hist),
      db()
{
    Datastore::Open(basename);
}

InMemory::~InMemory() {
    Close();
}

bool InMemory::OpenImpl(const std::string &) {
    return true;
}

void InMemory::CloseImpl() {
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
Response::BPut *InMemory::BPutImpl(void **subjects, std::size_t *subject_lens,
                                   void **predicates, std::size_t *predicate_lens,
                                   hxhim_type_t *, void **objects, std::size_t *object_lens,
                                   std::size_t count) {
    Response::BPut *ret = construct<Response::BPut>(count);

    struct timespec start = {};
    struct timespec end = {};

    for(std::size_t i = 0; i < count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        db[std::string((char *) key, key_len)] = std::string((char *) objects[i], object_lens[i]);
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

        stats.puts++;
        stats.put_times += nano(start, end);

        // always successful
        ret->statuses[i] = HXHIM_SUCCESS;
    }

    ret->count = count;

    stats.put_times += nano(start, end);

    return ret;
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
Response::BGet *InMemory::BGetImpl(void **subjects, std::size_t *subject_lens,
                                   void **predicates, std::size_t *predicate_lens,
                                   hxhim_type_t *object_types,
                                   std::size_t count) {
    Response::BGet *ret = construct<Response::BGet>(count);

    for(std::size_t i = 0; i < count; i++) {
        struct timespec start = {};
        struct timespec end = {};
        std::string value_str;

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

        ret->subjects[i] = construct<RealBlob>(subjects[i], subject_lens[i]);
        subjects[i] = nullptr;
        subject_lens[i] = 0;
        ret->predicates[i] = construct<RealBlob>(predicates[i], predicate_lens[i]);
        predicates[i] = nullptr;
        predicate_lens[i] = 0;

        ret->statuses[i] = (it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR;

        if (ret->statuses[i] == HXHIM_SUCCESS) {
            ret->object_types[i] = object_types[i];
            ret->objects[i] = construct<RealBlob>(it->second.size(), it->second.data());
        }

        stats.gets++;
        stats.get_times += nano(start, end);
    }

    ret->count = count;

    return ret;
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
Response::BGet2 *InMemory::BGetImpl2(void **subjects, std::size_t *subject_lens,
                                     void **predicates, std::size_t *predicate_lens,
                                     hxhim_type_t *object_types,
                                     void **orig_subjects,
                                     void **orig_predicates,
                                     void **orig_objects, std::size_t **orig_object_lens,
                                     std::size_t count) {
    // initialize to count of 0 in order to reuse arrays
    Response::BGet2 *ret = construct<Response::BGet2>(count);

    for(std::size_t i = 0; i < count; i++) {
        struct timespec start = {};
        struct timespec end = {};
        std::string value_str;

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

        // move data into ret
        ret->subjects[i] = construct<RealBlob>(subjects[i], subject_lens[i]);
        subjects[i] = nullptr;
        subject_lens[i] = 0;
        ret->predicates[i] = construct<RealBlob>(predicates[i], predicate_lens[i]);
        predicates[i] = nullptr;
        predicate_lens[i] = 0;
        ret->object_types[i] = object_types[i];

        // copy requester addresses
        ret->orig.subjects[i] = orig_subjects[i];
        ret->orig.predicates[i] = orig_predicates[i];
        ret->orig.objects[i] = orig_objects[i];
        ret->orig.object_lens[i] = orig_object_lens[i];

        ret->statuses[i] = (it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR;

        if (ret->statuses[i] == HXHIM_SUCCESS) {
            ret->objects[i] = construct<RealBlob>(it->second.size(), it->second.data());
        }

        stats.gets++;
        stats.get_times += nano(start, end);
    }

    return ret;
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
Response::BGetOp *InMemory::BGetOpImpl(void *subject, std::size_t subject_len,
                                       void *predicate, std::size_t predicate_len,
                                       hxhim_type_t object_type,
                                       std::size_t recs, enum hxhim_get_op_t op) {
    Response::BGetOp *ret = construct<Response::BGetOp>(recs);

    struct timespec start = {};
    struct timespec end = {};


    void *key = nullptr;
    std::size_t key_len = 0;
    sp_to_key(subject, subject_len, predicate, predicate_len, &key, &key_len);

    // add in the time to get the first key-value without adding to the counter
    clock_gettime(CLOCK_MONOTONIC, &start);
    decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
    clock_gettime(CLOCK_MONOTONIC, &end);

    dealloc(key);

    decltype(db)::const_reverse_iterator rit = std::make_reverse_iterator(it);

    stats.get_times += nano(start, end);

    if (it != db.end()) {
        for(std::size_t i = 0; i < recs && (it != db.end()) && (rit != db.rend()); i++) {
            ret->statuses[i] = (it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR;
            if (ret->statuses[i] == HXHIM_SUCCESS) {
                key_to_sp((void *) it->first.data(), it->first.size(), &ret->subjects[i]->ptr, &ret->subjects[i]->len, &ret->predicates[i]->ptr, &ret->predicates[i]->len);
                ret->object_types[i] = object_type;
                ret->objects[i]->ptr = alloc(it->second.size());
                memcpy(ret->objects[i]->ptr, it->second.data(), it->second.size());
            }

            clock_gettime(CLOCK_MONOTONIC, &start);

            // move to next iterator according to operation
            switch (op) {
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

        ret->count++;
    }

    return ret;
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
Response::BDelete *InMemory::BDeleteImpl(void **subjects, std::size_t *subject_lens,
                                         void **predicates, std::size_t *predicate_lens,
                                         std::size_t count) {
    Response::BDelete *ret = construct<Response::BDelete>(count);

    for(std::size_t i = 0; i < count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        if (it != db.end()) {
            db.erase(it);
            ret->statuses[i] = HXHIM_SUCCESS;
        }
        else {
            ret->statuses[i] = HXHIM_ERROR;
        }

        dealloc(key);
    }

    ret->count = count;

    return ret;
}

/**
 * Sync
 * NOOP
 *
 * @return HXHIM_SUCCESS
 */
int InMemory::SyncImpl() {
    return HXHIM_SUCCESS;
}

}
}
