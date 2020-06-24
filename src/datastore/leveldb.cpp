#include <cerrno>
#include <ctime>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>

#include "leveldb/write_batch.h"

#include "hxhim/accessors.hpp"
#include "datastore/leveldb.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/Blob.hpp"
#include "utils/elapsed.h"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

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

hxhim::datastore::leveldb::leveldb(hxhim_t *hx,
                                   Histogram::Histogram *hist,
                                   const std::string &exact_name)
    : Datastore(hx, 0, hist),
      create_if_missing(false),
      db(nullptr), options()
{
    if (!Datastore::Open(exact_name)) {
        throw std::runtime_error("Could not configure leveldb datastore " + exact_name);
    }

    mlog(LEVELDB_INFO, "Opened leveldb with name: %s", exact_name.c_str());
}

hxhim::datastore::leveldb::leveldb(hxhim_t *hx,
                                   const int id,
                                   Histogram::Histogram *hist,
                                   const std::string &prefix,
                                   const std::string &basename, const bool create_if_missing)
    : Datastore(hx, id, hist),
      create_if_missing(create_if_missing),
      db(nullptr), options()
{
    mkdir_p(prefix, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    std::stringstream s;
    s << prefix << "/" << basename << "-" << id;
    const std::string name = s.str();

    options.create_if_missing = create_if_missing;

    if (!Datastore::Open(name)) {
        throw std::runtime_error("Could not configure leveldb datastore " + name);
    }

    mlog(LEVELDB_INFO, "Opened leveldb with name: %s", s.str().c_str());
}

hxhim::datastore::leveldb::~leveldb() {
    Close();
}

bool hxhim::datastore::leveldb::OpenImpl(const std::string &new_name) {
    return ::leveldb::DB::Open(options, new_name, &db).ok();
}

void hxhim::datastore::leveldb::CloseImpl() {
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
Transport::Response::BPut *hxhim::datastore::leveldb::BPutImpl(Transport::Request::BPut *req) {
    mlog(LEVELDB_INFO, "LevelDB BPut");
    Transport::Response::BPut *res = construct<Transport::Response::BPut>(req->count);

    struct timespec start, end;
    ::leveldb::WriteBatch batch;

    for(std::size_t i = 0; i < req->count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i]->ptr, req->subjects[i]->len, req->predicates[i]->ptr, req->predicates[i]->len, &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        batch.Put(::leveldb::Slice((char *) key, key_len), ::leveldb::Slice((char *) req->objects[i]->ptr, req->objects[i]->len));
        clock_gettime(CLOCK_MONOTONIC, &end);

        // save requesting addresses for sending back
        res->orig.subjects[i]   = construct<ReferenceBlob>(req->orig.subjects[i], req->subjects[i]->len);
        res->orig.predicates[i] = construct<ReferenceBlob>(req->orig.predicates[i], req->predicates[i]->len);

        dealloc(key);

        stats.puts++;
        stats.put_times += nano(start, end);
    }

    // add in the time to write the key-value pairs without adding to the counter
    clock_gettime(CLOCK_MONOTONIC, &start);
    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);
    clock_gettime(CLOCK_MONOTONIC, &end);

    if (status.ok()) {
        for(std::size_t i = 0; i < req->count; i++) {
            res->statuses[i] = HXHIM_SUCCESS;
        }
        mlog(LEVELDB_INFO, "LevelDB write success");
    }
    else {
        for(std::size_t i = 0; i < req->count; i++) {
            res->statuses[i] = HXHIM_ERROR;
        }
        mlog(LEVELDB_INFO, "LevelDB write error");
    }

    res->count = req->count;
    stats.put_times += nano(start, end);

    mlog(LEVELDB_INFO, "LevelDB BPut Completed");
    return res;
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
Transport::Response::BGet *hxhim::datastore::leveldb::BGetImpl(Transport::Request::BGet *req) {
    int rank = -1;
    hxhim::GetMPIRank(hx, &rank);

    mlog(LEVELDB_INFO, "Rank %d LevelDB GET processing %zu item in %ps", rank, req->count, req);
    Transport::Response::BGet *res = construct<Transport::Response::BGet>(req->count);
    for(std::size_t i = 0; i < req->count; i++) {
        mlog(LEVELDB_INFO, "Rank %d LevelDB GET processing %p[%zu] = {%p, %p}", rank, req, i, req->subjects[i], req->predicates[i]);
        struct timespec start, end;

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i]->ptr, req->subjects[i]->len, req->predicates[i]->ptr, req->predicates[i]->len, &key, &key_len);

        // create the key
        const ::leveldb::Slice k((char *) key, key_len);

        // get the value
        clock_gettime(CLOCK_MONOTONIC, &start);
        ::leveldb::Slice value; // read gotten value into a Slice instead of a std::string to save a few copies
        ::leveldb::Status status = db->Get(::leveldb::ReadOptions(), k, value);
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

        res->ds_offsets[i]       = req->ds_offsets[i];

        // object type was stored as a value, not address, so copy it to the response
        res->object_types[i]     = req->object_types[i];

        // save requesting addresses for sending back
        res->orig.subjects[i]    = construct<ReferenceBlob>(req->orig.subjects[i], req->subjects[i]->len);
        res->orig.predicates[i]  = construct<ReferenceBlob>(req->orig.predicates[i], req->predicates[i]->len);
        res->orig.objects[i]     = req->orig.objects[i];
        res->orig.object_lens[i] = req->orig.object_lens[i];

        // put object into response
        if (status.ok()) {
            mlog(LEVELDB_INFO, "Rank %d LevelDB GET success", rank);
            res->statuses[i] = HXHIM_SUCCESS;
            res->objects[i] = construct<RealBlob>(value.size(), value.data());
        }
        else {
            mlog(LEVELDB_WARN, "Rank %d LevelDB GET error: %s", rank, status.ToString().c_str());
            res->statuses[i] = HXHIM_ERROR;
        }

        res->count++;

        // update stats
        stats.gets++;
        stats.get_times += nano(start, end);
    }

    mlog(LEVELDB_INFO, "Rank %d LevelDB GET done processing %p", rank, req);
    return res;
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
Transport::Response::BGetOp *hxhim::datastore::leveldb::BGetOpImpl(Transport::Request::BGetOp *req) {
    Transport::Response::BGetOp *res = construct<Transport::Response::BGetOp>(0);
    Transport::Response::BGetOp *curr = res;

    for(std::size_t i = 0; i < req->count; i++) {
        Transport::Response::BGetOp *response = construct<Transport::Response::BGetOp>(req->num_recs[i]);

        ::leveldb::Iterator *it = db->NewIterator(::leveldb::ReadOptions());
        struct timespec start, end;

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i]->ptr, req->subjects[i]->len, req->predicates[i]->ptr, req->predicates[i]->len, &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        it->Seek(::leveldb::Slice((char *) key, key_len));
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

        // add in the time to get the first key-value without adding to the counter
        stats.get_times += nano(start, end);

        if (it->status().ok()) {
            for(std::size_t j = 0; j < req->num_recs[i] && it->Valid(); j++) {
                clock_gettime(CLOCK_MONOTONIC, &start);
                const ::leveldb::Slice k = it->key();
                const ::leveldb::Slice v = it->value();
                clock_gettime(CLOCK_MONOTONIC, &end);

                stats.gets++;
                stats.get_times += nano(start, end);

                // add to results list
                if (it->status().ok()) {
                    response->statuses[j] = HXHIM_SUCCESS;

                    void *subject = nullptr, *predicate = nullptr;
                    key_to_sp(k.data(), k.size(), &subject, &response->subjects[j]->len, &predicate, &response->predicates[j]->len);

                    // need to copy subject out of the key
                    response->subjects[j] = construct<RealBlob>(subject, response->subjects[j]->len);

                    // need to copy predicate out of the key
                    response->predicates[j] = construct<RealBlob>(predicate, response->predicates[j]->len);

                    response->object_types[j] = req->object_types[i];
                    response->objects[j]->len = v.size();
                    response->objects[j]->ptr = alloc(response->objects[j]->len);
                    memcpy(response->objects[j]->ptr, v.data(), response->objects[j]->len);
                }
                else {
                    response->statuses[j] = HXHIM_ERROR;
                }
                response->count++;

                // move to next iterator according to operation
                switch (req->ops[i]) {
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

            curr = (curr->next = response);
        }

        delete it;
    }

    // first result is empty
    return res->next;
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
Transport::Response::BDelete *hxhim::datastore::leveldb::BDeleteImpl(Transport::Request::BDelete *req) {
    Transport::Response::BDelete *res = construct<Transport::Response::BDelete>(req->count);

    ::leveldb::WriteBatch batch;

    // batch delete
    for(std::size_t i = 0; i < req->count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i]->ptr, req->subjects[i]->len, req->predicates[i]->ptr, req->predicates[i]->len, &key, &key_len);

        batch.Delete(::leveldb::Slice((char *) key, key_len));

        res->orig.subjects[i]   = construct<ReferenceBlob>(req->orig.subjects[i], req->subjects[i]->len);
        res->orig.predicates[i] = construct<ReferenceBlob>(req->orig.predicates[i], req->predicates[i]->len);

        dealloc(key);
    }

    // create responses
    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);
    const int stat = status.ok()?HXHIM_SUCCESS:HXHIM_ERROR;
    for(std::size_t i = 0; i < req->count; i++) {
        res->statuses[i] = stat;
    }

    res->count = req->count;

    return res;
}

/**
 * Sync
 * Syncs the database to disc
 *
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::datastore::leveldb::SyncImpl() {
    ::leveldb::WriteBatch batch;
    ::leveldb::WriteOptions options;
    options.sync = true;
    return db->Write(options, &batch).ok()?HXHIM_SUCCESS:HXHIM_ERROR;
}
