#include <ctime>
#include <sstream>
#include <stdexcept>

#include "datastore/InMemory.hpp"
#include "hxhim/triplestore.hpp"

namespace hxhim {
namespace datastore {

using namespace Transport;

InMemory::InMemory(hxhim_t *hx,
                   const int id,
                   const std::size_t use_first_n, const Histogram::BucketGen::generator &generator, void *extra_args)
    : Datastore(hx, id, use_first_n, generator, extra_args),
      db()
{}

InMemory::~InMemory() {
    Close();
}

void InMemory::Close() {
    db.clear();
}

/**
 * StatFlush
 * NOOP
 *
 * @return HXHIM_ERROR
 */
int InMemory::StatFlush() {
    return HXHIM_ERROR;
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
                                   hxhim_type_t *object_types, void **objects, std::size_t *object_lens,
                                   std::size_t count) {
    Response::BPut *ret = new Response::BPut(count);
    if (!ret) {
        return nullptr;
    }

    struct timespec start, end;

    for(std::size_t i = 0; i < count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        db[std::string((char *) key, key_len)] = std::string((char *) objects[i], object_lens[i]);
        clock_gettime(CLOCK_MONOTONIC, &end);

        ::operator delete(key);

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
    Response::BGet *ret = new Response::BGet(count);
    if (!ret) {
        return nullptr;
    }

    for(std::size_t i = 0; i < count; i++) {
        struct timespec start, end;
        std::string value_str;

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        clock_gettime(CLOCK_MONOTONIC, &end);

        ::operator delete(key);

        stats.gets++;
        stats.get_times += nano(start, end);

        ret->statuses[i] = (it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR;

        ret->subject_lens[i] = subject_lens[i];
        ret->subjects[i] = ::operator new(ret->subject_lens[i]);
        memcpy(ret->subjects[i], subjects[i], ret->subject_lens[i]);

        ret->predicate_lens[i] = predicate_lens[i];
        ret->predicates[i] = ::operator new(ret->predicate_lens[i]);
        memcpy(ret->predicates[i], predicates[i], ret->predicate_lens[i]);

        ret->object_types[i] = object_types[i];

        if (ret->statuses[i] == HXHIM_SUCCESS) {
            ret->objects[i] = ::operator new(it->second.size());
            memcpy(ret->objects[i], it->second.data(), it->second.size());
        }
    }

    ret->count = count;

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
    Response::BGetOp *ret = new Response::BGetOp(recs);
    if (!ret) {
        return nullptr;
    }

    struct timespec start, end;

    void *key = nullptr;
    std::size_t key_len = 0;
    sp_to_key(subject, subject_len, predicate, predicate_len, &key, &key_len);

    // add in the time to get the first key-value without adding to the counter
    clock_gettime(CLOCK_MONOTONIC, &start);
    decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
    clock_gettime(CLOCK_MONOTONIC, &end);

    ::operator delete(key);

    decltype(db)::const_reverse_iterator rit = std::make_reverse_iterator(it);

    stats.get_times += nano(start, end);

    if (it != db.end()) {
        for(std::size_t i = 0; i < recs && (it != db.end()) && (rit != db.rend()); i++) {
            ret->statuses[i] = (it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR;
            if (ret->statuses[i] == HXHIM_SUCCESS) {
                key_to_sp((void *) it->first.data(), it->first.size(), &ret->subjects[i], &ret->subject_lens[i], &ret->predicates[i], &ret->predicate_lens[i]);
                ret->object_types[i] = object_type;
                ret->objects[i] = ::operator new(it->second.size());
                memcpy(ret->objects[i], it->second.data(), it->second.size());
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
    Response::BDelete *ret = new Response::BDelete(count);
    if (!ret) {
        return nullptr;
    }

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

        ::operator delete(key);
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

std::ostream &InMemory::print_config(std::ostream &stream) const {
    return stream;
}

}
}
