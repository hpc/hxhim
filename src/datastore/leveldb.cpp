#include <cerrno>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>

#include "leveldb/write_batch.h"

#include "hxhim/accessors.hpp"
#include "datastore/leveldb.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"
#include "utils/triplestore.hpp"

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

hxhim::datastore::leveldb::leveldb(const int rank,
                                   Histogram::Histogram *hist,
                                   const std::string &exact_name,
                                   const bool create_if_missing)
    : Datastore(rank, 0, hist),
      dbname(exact_name),
      create_if_missing(create_if_missing),
      db(nullptr), options()
{
    options.create_if_missing = create_if_missing;

    Datastore::Open(dbname);

    mlog(LEVELDB_INFO, "Opened leveldb with name: %s", exact_name.c_str());
}

hxhim::datastore::leveldb::leveldb(const int rank,
                                   const int id,
                                   Histogram::Histogram *hist,
                                   const std::string &prefix,
                                   const std::string &basename,
                                   const bool create_if_missing)
    : Datastore(rank, id, hist),
      dbname(),
      create_if_missing(create_if_missing),
      db(nullptr), options()
{
    mkdir_p(prefix, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    std::stringstream s;
    s << prefix << "/" << basename << "-" << id;
    dbname = s.str();

    options.create_if_missing = create_if_missing;

    Datastore::Open(dbname);

    mlog(LEVELDB_INFO, "Opened leveldb with name: %s", dbname.c_str());
}

hxhim::datastore::leveldb::~leveldb() {
    Close();
}

const std::string &hxhim::datastore::leveldb::name() const {
    return dbname;
}

bool hxhim::datastore::leveldb::OpenImpl(const std::string &new_name) {
    ::leveldb::Status status = ::leveldb::DB::Open(options, new_name, &db);
    if (!status.ok()) {
        throw std::runtime_error("Could not configure leveldb datastore " + new_name + ": " + status.ToString());
    }
    return status.ok();
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

    hxhim::datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BPut *res = construct<Transport::Response::BPut>(req->count);

    // batch up PUTs
    ::leveldb::WriteBatch batch;
    for(std::size_t i = 0; i < req->count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i], req->predicates[i], &key, &key_len);

        batch.Put(::leveldb::Slice((char *) key, key_len), ::leveldb::Slice((char *) req->objects[i].data(), req->objects[i].size()));

        // save requesting addresses for sending back
        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size());

        dealloc(key);

        event.size += key_len + req->objects[i].size();
    }

    // add in the time to write the key-value pairs without adding to the counter
    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);

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
    event.time.end = ::Stats::now();
    stats.puts.emplace_back(event);

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
    mlog(LEVELDB_INFO, "Rank %d LevelDB GET processing %zu item in %ps", rank, req->count, req);

    hxhim::datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BGet *res = construct<Transport::Response::BGet>(req->count);

    // batch up GETs
    for(std::size_t i = 0; i < req->count; i++) {
        mlog(LEVELDB_INFO, "Rank %d LevelDB GET processing %p[%zu] = {%p, %p}", rank, req, i, req->subjects[i].data(), req->predicates[i].data());

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i], req->predicates[i], &key, &key_len);

        // create the key
        const ::leveldb::Slice k((char *) key, key_len);

        // get the value
        ::leveldb::Slice value; // read gotten value into a Slice instead of a std::string to save a few copies
        ::leveldb::Status status = db->Get(::leveldb::ReadOptions(), k, value);

        dealloc(key);

        res->ds_offsets[i]      = req->ds_offsets[i];

        // object type was stored as a value, not address, so copy it to the response
        res->object_types[i]    = req->object_types[i];

        // save requesting addresses for sending back
        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size());
        event.size += key_len;

        // put object into response
        if (status.ok()) {
            mlog(LEVELDB_INFO, "Rank %d LevelDB GET success", rank);
            res->statuses[i] = HXHIM_SUCCESS;
            res->objects[i] = RealBlob(value.size(), value.data());

            event.size += res->objects[i].size();
        }
        else {
            mlog(LEVELDB_INFO, "Rank %d LevelDB GET error: %s", rank, status.ToString().c_str());
            res->statuses[i] = HXHIM_ERROR;
        }

        res->count++;
    }

    event.time.end = ::Stats::now();
    stats.gets.emplace_back(event);

    mlog(LEVELDB_INFO, "Rank %d LevelDB GET done processing %p", rank, req);
    return res;
}

static void BGetOp_copy_response(const ::leveldb::Iterator *it,
                                 Transport::Response::BGetOp *res,
                                 const std::size_t i,
                                 const std::size_t j,
                                 hxhim::datastore::Datastore::Stats::Event &event) {
    const ::leveldb::Slice k = it->key();
    const ::leveldb::Slice v = it->value();

    // copy key into subject/predicate
    key_to_sp(k.data(), k.size(), res->subjects[i][j], res->predicates[i][j], true);

    // copy object
    res->objects[i][j] = RealBlob(alloc(v.size()), v.size());
    memcpy(res->objects[i][j].data(), v.data(), v.size());

    res->num_recs[i]++;

    event.size += k.size() + res->objects[i][j].size();
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

    Transport::Response::BGetOp *res = construct<Transport::Response::BGetOp>(req->count);

    ::leveldb::Iterator *it = db->NewIterator(::leveldb::ReadOptions());

    for(std::size_t i = 0; i < req->count; i++) {
        hxhim::datastore::Datastore::Stats::Event event;
        event.time.start = ::Stats::now();

        // prepare response
        res->object_types[i] = req->object_types[i];
        res->num_recs[i]     = 0;
        res->subjects[i]     = alloc_array<Blob>(req->num_recs[i]);
        res->predicates[i]   = alloc_array<Blob>(req->num_recs[i]);
        res->objects[i]      = alloc_array<Blob>(req->num_recs[i]);

        if (req->ops[i] == hxhim_get_op_t::HXHIM_GET_EQ) {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(req->subjects[i], req->predicates[i], &key, &key_len);

            it->Seek(::leveldb::Slice((char *) key, key_len));

            dealloc(key);

            // only 1 response, so j == 0 (num_recs is ignored)
            BGetOp_copy_response(it, res, i, 0, event);
        }
        else if (req->ops[i] == hxhim_get_op_t::HXHIM_GET_NEXT) {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(req->subjects[i], req->predicates[i], &key, &key_len);

            it->Seek(::leveldb::Slice((char *) key, key_len));

            dealloc(key);

            // first result returned is (subject, predicate)
            // (results are offsets)
            for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                BGetOp_copy_response(it, res, i, j, event);
                it->Next();
            }
        }
        else if (req->ops[i] == hxhim_get_op_t::HXHIM_GET_PREV) {
            void *key = nullptr;
            std::size_t key_len = 0;
            sp_to_key(req->subjects[i], req->predicates[i], &key, &key_len);

            it->Seek(::leveldb::Slice((char *) key, key_len));

            dealloc(key);

            // first result returned is (subject, predicate)
            // (results are offsets)
            for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                BGetOp_copy_response(it, res, i, j, event);
                it->Prev();
            }
        }
        else if (req->ops[i] == hxhim_get_op_t::HXHIM_GET_FIRST) {
            // ignore key
            it->SeekToFirst();

            // first result returned is (subject, predicate)
            // (results are offsets)
            for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                BGetOp_copy_response(it, res, i, j, event);
                it->Next();
            }
        }
        else if (req->ops[i] == hxhim_get_op_t::HXHIM_GET_LAST) {
            // ignore key
            it->SeekToLast();

            // first result returned is (subject, predicate)
            // (results are offsets)
            for(std::size_t j = 0; (j < req->num_recs[i]) && it->Valid(); j++) {
                BGetOp_copy_response(it, res, i, j, event);
                it->Prev();
            }
        }

        // all responses for this Op share a status
        res->statuses[i] = it->status().ok()?HXHIM_SUCCESS:HXHIM_ERROR;

        res->count++;

        event.count = res->num_recs[i];
        event.time.end = ::Stats::now();
        stats.getops.emplace_back(event);
    }

    delete it;

    return res;
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
    hxhim::datastore::Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BDelete *res = construct<Transport::Response::BDelete>(req->count);

    ::leveldb::WriteBatch batch;

    // batch delete
    for(std::size_t i = 0; i < req->count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i], req->predicates[i], &key, &key_len);

        batch.Delete(::leveldb::Slice((char *) key, key_len));

        res->orig.subjects[i]   = ReferenceBlob(req->orig.subjects[i], req->subjects[i].size());
        res->orig.predicates[i] = ReferenceBlob(req->orig.predicates[i], req->predicates[i].size());

        dealloc(key);

        event.size += key_len;
    }

    // create responses
    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);
    const int stat = status.ok()?HXHIM_SUCCESS:HXHIM_ERROR;
    for(std::size_t i = 0; i < req->count; i++) {
        res->statuses[i] = stat;
    }

    res->count = req->count;

    event.time.end = ::Stats::now();
    stats.deletes.emplace_back(event);

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
