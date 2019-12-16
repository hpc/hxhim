#if HXHIM_HAVE_LEVELDB

#include <cerrno>
#include <ctime>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>

#include "leveldb/write_batch.h"

#include "datastore/leveldb.hpp"
#include "hxhim/private.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/macros.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace hxhim {
namespace datastore {

using namespace Transport;

static int mkdir_p(const std::string & path, const mode_t mode, const char sep = '/') {
    char * copy = new char[path.size() + 1]();
    memcpy(copy, path.c_str(), path.size());
    copy[path.size()] = '\0';

    size_t i = 1;
    while (i < path.size()) {
        while ((i < path.size()) &&
               (copy[i] != sep)) {
            i++;
        }

        copy[i] = '\0';

        // build current path
        if (mkdir(copy, mode) != 0) {
            const int err = errno;

            // if the error was not caused by the path existing, error
            if (err != EEXIST) {
                delete [] copy;
                return err;
            }
        }

        copy[i] = sep;
        i++;
    }

    delete [] copy;

    return 0;
}

leveldb::leveldb(hxhim_t *hx,
                 Histogram::Histogram *hist,
                 const std::string &exact_name)
    : Datastore(hx, 0, hist),
      create_if_missing(false),
      db(nullptr), options()
{
    mkdir_p(hx->p->datastore.prefix, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    if (!Datastore::Open(exact_name)) {
        throw std::runtime_error("Could not configure leveldb datastore " + exact_name);
    }

    mlog(LEVELDB_INFO, "Opened leveldb with name: %s", exact_name.c_str());
}

leveldb::leveldb(hxhim_t *hx,
                 const int id,
                 Histogram::Histogram *hist,
                 const std::string &basename, const bool create_if_missing)
    : Datastore(hx, id, hist),
      create_if_missing(create_if_missing),
      db(nullptr), options()
{
    mkdir_p(hx->p->datastore.prefix, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    std::stringstream s;
    s << hx->p->datastore.prefix << "/" << basename << "-" << id;
    const std::string name = s.str();

    options.create_if_missing = create_if_missing;

    if (!Datastore::Open(name)) {
        throw std::runtime_error("Could not configure leveldb datastore " + name);
    }

    mlog(LEVELDB_INFO, "Opened leveldb with name: %s", s.str().c_str());
}

leveldb::~leveldb() {
    Close();
}

bool leveldb::OpenImpl(const std::string &new_name) {
    return ::leveldb::DB::Open(options, new_name, &db).ok();
}

void leveldb::CloseImpl() {
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
Response::BPut *leveldb::BPutImpl(void **subjects, std::size_t *subject_lens,
                                  void **predicates, std::size_t *predicate_lens,
                                  hxhim_type_t *, void **objects, std::size_t *object_lens,
                                  std::size_t count) {
    mlog(LEVELDB_INFO, "LevelDB BPut");
    Response::BPut *ret = construct<Response::BPut>(count);

    struct timespec start, end;
    ::leveldb::WriteBatch batch;

    for(std::size_t i = 0; i < count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        batch.Put(::leveldb::Slice((char *) key, key_len), ::leveldb::Slice((char *) objects[i], object_lens[i]));
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

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
        mlog(LEVELDB_INFO, "LevelDB write success");
    }
    else {
        for(std::size_t i = 0; i < count; i++) {
            ret->statuses[i] = HXHIM_ERROR;
        }
        mlog(LEVELDB_INFO, "LevelDB write error");
    }

    ret->count = count;
    stats.put_times += nano(start, end);

    mlog(LEVELDB_INFO, "LevelDB BPut Completed");
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
    Response::BGet *ret = construct<Response::BGet>(count);

    for(std::size_t i = 0; i < count; i++) {
        struct timespec start, end;
        ::leveldb::Slice value; // read gotten value into a Slice instead of a std::string to save a few copies

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        // create the key
        const ::leveldb::Slice k((char *) key, key_len);

        // get the value
        clock_gettime(CLOCK_MONOTONIC, &start);
        ::leveldb::Status status = db->Get(::leveldb::ReadOptions(), k, value);
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

        // need to copy subject
        ret->subjects[i]->len = subject_lens[i];
        ret->subjects[i]->ptr = alloc(ret->subjects[i]->len);
        memcpy(ret->subjects[i]->ptr, subjects[i], ret->subjects[i]->len);

        // need to copy predicate
        ret->predicates[i]->len = predicate_lens[i];
        ret->predicates[i]->ptr = alloc(ret->predicates[i]->len);
        memcpy(ret->predicates[i]->ptr, predicates[i], ret->predicates[i]->len);

        // add to results list
        if (status.ok()) {
            ret->statuses[i] = HXHIM_SUCCESS;

            ret->object_types[i] = object_types[i];
            ret->objects[i]->len = value.size();
            ret->objects[i]->ptr = alloc(ret->objects[i]->len);
            memcpy(ret->objects[i]->ptr, value.data(), ret->objects[i]->len);
        }
        else {
            ret->statuses[i] = HXHIM_ERROR;
        }

        // update stats
        stats.gets++;
        stats.get_times += nano(start, end);
    }

    ret->count = count;

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
Response::BGet2 *leveldb::BGetImpl2(void **subjects, std::size_t *subject_lens,
                                    void **predicates, std::size_t *predicate_lens,
                                    hxhim_type_t *object_types,
                                    void **orig_subjects,
                                    void **orig_predicates,
                                    void **orig_objects, std::size_t **orig_object_lens,
                                    std::size_t count) {
    Response::BGet2 *ret = construct<Response::BGet2>(count);
    for(std::size_t i = 0; i < count; i++) {
        struct timespec start, end;

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        // create the key
        const ::leveldb::Slice k((char *) key, key_len);

        // get the value
        clock_gettime(CLOCK_MONOTONIC, &start);
        ::leveldb::Slice value; // read gotten value into a Slice instead of a std::string to save a few copies
        ::leveldb::Status status = db->Get(::leveldb::ReadOptions(), k, value);
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

        // move data into ret
        ret->subjects[i]->ptr = construct<RealBlob>(subjects[i], subject_lens[i]);
        subjects[i] = nullptr;
        subject_lens[i] = 0;
        ret->predicates[i]->ptr = construct<RealBlob>(predicates[i], predicate_lens[i]);
        predicates[i] = nullptr;
        predicate_lens[i] = 0;
        ret->object_types[i] = object_types[i];

        // move client addresses
        ret->orig.subjects[i] = orig_subjects[i];
        ret->orig.predicates[i] = orig_predicates[i];
        ret->orig.objects[i] = orig_objects[i];
        ret->orig.object_lens[i] = orig_object_lens[i];

        ret->objects[i] = construct<RealBlob>();

        // copy object
        if (status.ok()) {
            ret->statuses[i] = HXHIM_SUCCESS;
            ret->objects[i]->len = value.size();
            ret->objects[i]->ptr = alloc(ret->objects[i]->len);
            memcpy(ret->objects[i]->ptr, value.data(), ret->objects[i]->len);
        }
        else {
            ret->statuses[i] = HXHIM_ERROR;
        }

        ret->count ++;

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
    Response::BGetOp *ret = construct<Response::BGetOp>(recs);

    ::leveldb::Iterator *it = db->NewIterator(::leveldb::ReadOptions());
    struct timespec start, end;

    void *key = nullptr;
    std::size_t key_len = 0;
    sp_to_key(subject, subject_len, predicate, predicate_len, &key, &key_len);

    clock_gettime(CLOCK_MONOTONIC, &start);
    it->Seek(::leveldb::Slice((char *) key, key_len));
    clock_gettime(CLOCK_MONOTONIC, &end);

    dealloc(key);

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

                void *subject = nullptr, *predicate = nullptr;
                key_to_sp(k.data(), k.size(), &subject, &ret->subjects[i]->len, &predicate, &ret->predicates[i]->len);

                // need to copy subject out of the key
                ret->subjects[i]->ptr = alloc(ret->subjects[i]->len);
                memcpy(ret->subjects[i]->ptr, subject, ret->subjects[i]->len);

                // need to copy predicate out of the key
                ret->predicates[i]->ptr = alloc(ret->predicates[i]->len);
                memcpy(ret->predicates[i]->ptr, predicate, ret->predicates[i]->len);

                ret->object_types[i] = object_type;
                ret->objects[i]->len = v.size();
                ret->objects[i]->ptr = alloc(ret->objects[i]->len);
                memcpy(ret->objects[i]->ptr, v.data(), ret->objects[i]->len);
            }
            else {
                ret->statuses[i] = HXHIM_ERROR;
            }
            ret->count++;

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
    Response::BDelete *ret = construct<Response::BDelete>(count);

    ::leveldb::WriteBatch batch;

    // batch delete
    for(std::size_t i = 0; i < count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &key, &key_len);

        batch.Delete(::leveldb::Slice((char *) key, key_len));

        dealloc(key);
    }

    // create responses
    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);
    const int stat = status.ok()?HXHIM_SUCCESS:HXHIM_ERROR;
    for(std::size_t i = 0; i < count; i++) {
        ret->statuses[i] = stat;
    }

    ret->count = count;

    return ret;
}

/**
 * Sync
 * Syncs the database to disc
 *
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int leveldb::SyncImpl() {
    ::leveldb::WriteBatch batch;
    ::leveldb::WriteOptions options;
    options.sync = true;
    return db->Write(options, &batch).ok()?HXHIM_SUCCESS:HXHIM_ERROR;
}

}
}

#endif
