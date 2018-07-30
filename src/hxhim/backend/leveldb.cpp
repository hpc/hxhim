#include <ctime>
#include <sstream>
#include <stdexcept>

#include "leveldb/write_batch.h"

#include "hxhim/backend/leveldb.hpp"
#include "hxhim/triplestore.hpp"

namespace hxhim {
namespace backend {

using namespace Transport;

leveldb::leveldb(hxhim_t *hx,
                 const std::size_t use_first_n, const Histogram::BucketGen::generator &generator, void *extra_args,
                 const std::string &exact_name)
    : base(hx, 0, use_first_n, generator, extra_args),
      name(exact_name), create_if_missing(false),
      db(nullptr), options()
{
    if (!::leveldb::DB::Open(options, exact_name, &db).ok()) {
        throw std::runtime_error("Could not configure leveldb backend " + exact_name);
    }
}

leveldb::leveldb(hxhim_t *hx,
                 const int id,
                 const std::size_t use_first_n, const Histogram::BucketGen::generator &generator, void *extra_args,
                 const std::string &name, const bool create_if_missing)
    : base(hx, id, use_first_n, generator, extra_args),
      name(name), create_if_missing(create_if_missing),
      db(nullptr), options()
{
    std::stringstream s;
    s << name << "-" << id;

    options.create_if_missing = create_if_missing;

    if (!::leveldb::DB::Open(options, s.str(), &db).ok()) {
        throw std::runtime_error("Could not configure leveldb backend " + s.str());
    }
}

leveldb::~leveldb() {
    Close();
}

void leveldb::Close() {
    delete db;
    db = nullptr;
}

/**
 * Commit
 * Syncs the database to disc
 *
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int leveldb::Commit() {
    ::leveldb::WriteBatch batch;
    ::leveldb::WriteOptions options;
    options.sync = true;
    return db->Write(options, &batch).ok()?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * StatFlush
 * NOOP
 *
 * @return HXHIM_ERROR
 */
int leveldb::StatFlush() {
    return HXHIM_ERROR;
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
Response::BPut *leveldb::BPutImpl(void **subjects, std::size_t *subject_lens,
                                  void **predicates, std::size_t *predicate_lens,
                                  hxhim_type_t *object_types, void **objects, std::size_t *object_lens,
                                  std::size_t count) {
    Response::BPut *ret = new Response::BPut(count);
    if (!ret) {
        return nullptr;
    }

    struct timespec start, end;
    ::leveldb::WriteBatch batch;

    for(std::size_t i = 0; i < count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        batch.Put(::leveldb::Slice((char *) key, key_len), ::leveldb::Slice((char *) objects[i], object_lens[i]));
        clock_gettime(CLOCK_MONOTONIC, &end);

        ::operator delete(key);

        stats.puts++;
        stats.put_times += nano(start, end);
    }

    // add in the time to write the key-value pairs without adding to the counter
    clock_gettime(CLOCK_MONOTONIC, &start);
    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);
    clock_gettime(CLOCK_MONOTONIC, &end);

    if (status.ok()) {
        for(std::size_t i = 0; i < count; i++) {
            ret->statuses[i] = HXHIM_SUCCESS;
        }
    }
    else {
        for(std::size_t i = 0; i < count; i++) {
            ret->statuses[i] = HXHIM_ERROR;
        }
    }

    stats.put_times += nano(start, end);

    return ret;
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
Response::BGet *leveldb::BGetImpl(void **subjects, std::size_t *subject_lens,
                                  void **predicates, std::size_t *predicate_lens,
                                  hxhim_type_t *object_types,
                                  std::size_t count) {
    Response::BGet *ret = new Response::BGet(count);
    if (!ret) {
        return nullptr;
    }

    for(std::size_t i = 0; i < count; i++) {
        struct timespec start, end;
        std::string value;

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        // create the key
        const ::leveldb::Slice k((char *) key, key_len);

        // get the value
        clock_gettime(CLOCK_MONOTONIC, &start);
        ::leveldb::Status status = db->Get(::leveldb::ReadOptions(), k, &value);
        clock_gettime(CLOCK_MONOTONIC, &end);

        // add to results list
        if (status.ok()) {
            ret->statuses[i] = HXHIM_SUCCESS;
            key_to_sp(key, key_len, &ret->subjects[i], &ret->subject_lens[i], &ret->predicates[i], &ret->predicate_lens[i]);
            ret->object_types[i] = object_types[i];
            ret->object_lens[i] = value.size();
            ret->objects[i] = ::operator new(ret->object_lens[i]);
            memcpy(ret->objects, value.data(), ret->object_lens[i]);
        }
        else {
            ret->statuses[i] = HXHIM_ERROR;
        }

        ::operator delete(key);

        // update stats
        stats.gets++;
        stats.get_times += nano(start, end);
    }

    return ret;
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
Response::BGetOp *leveldb::BGetOpImpl(void *subject, std::size_t subject_len,
                                      void *predicate, std::size_t predicate_len,
                                      hxhim_type_t object_type,
                                      std::size_t recs, enum hxhim_get_op_t op) {
    Response::BGetOp *ret = new Response::BGetOp(recs);
    if (!ret) {
        return nullptr;
    }

    ::leveldb::Iterator *it = db->NewIterator(::leveldb::ReadOptions());
    struct timespec start, end;

    void *key = nullptr;
    std::size_t key_len = 0;
    sp_to_key(subject, subject_len, predicate, predicate_len, &key, &key_len);

    clock_gettime(CLOCK_MONOTONIC, &start);
    it->Seek(::leveldb::Slice((char *) key, key_len));
    clock_gettime(CLOCK_MONOTONIC, &end);

    ::operator delete(key);

    // add in the time to get the first key-value without adding to the counter
    stats.get_times += nano(start, end);

    if (it->status().ok()) {
        for(std::size_t i = 0; i < recs && it->Valid(); i++) {
            clock_gettime(CLOCK_MONOTONIC, &start);
            const ::leveldb::Slice k = it->key();
            const ::leveldb::Slice v = it->value();
            clock_gettime(CLOCK_MONOTONIC, &end);

            stats.gets++;
            stats.get_times += nano(start, end);

            // add to results list
            if (it->status().ok()) {
                ret->statuses[i] = HXHIM_SUCCESS;
                key_to_sp(k.data(), k.size(), &ret->subjects[i], &ret->subject_lens[i], &ret->predicates[i], &ret->predicate_lens[i]);
                ret->object_types[i] = object_type;
                ret->object_lens[i] = v.size();
                ret->objects[i] = ::operator new(ret->object_lens[i]);
                memcpy(ret->objects, v.data(), ret->object_lens[i]);
            }
            else {
                ret->statuses[i] = HXHIM_ERROR;
            }

            // move to next iterator according to operation
            switch (op) {
                case hxhim_get_op_t::HXHIM_GET_NEXT:
                    it->Next();
                    break;
                case hxhim_get_op_t::HXHIM_GET_PREV:
                    it->Prev();
                    break;
                case hxhim_get_op_t::HXHIM_GET_FIRST:
                    it->Next();
                    break;
                case hxhim_get_op_t::HXHIM_GET_LAST:
                    it->Prev();
                    break;
                default:
                    break;
            }
        }
    }

    delete it;

    return ret;
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
Response::BDelete *leveldb::BDeleteImpl(void **subjects, std::size_t *subject_lens,
                                        void **predicates, std::size_t *predicate_lens,
                                        std::size_t count) {
    Response::BDelete *ret = new Response::BDelete(count);
    ::leveldb::WriteBatch batch;

    // batch delete
    for(std::size_t i = 0; i < count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        batch.Delete(::leveldb::Slice((char *) key, key_len));

        ::operator delete(key);
    }

    // create responses
    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);
    const int stat = status.ok()?HXHIM_SUCCESS:HXHIM_ERROR;
    for(std::size_t i = 0; i < count; i++) {
        ret->statuses[i] = stat;
    }

    return ret;
}

std::ostream &leveldb::print_config(std::ostream &stream) const {
    return stream
        << "leveldb" << std::endl
        << "    name: " << name << std::endl
        << "    create_if_missing: " << std::boolalpha << create_if_missing << std::endl;
}

}
}
