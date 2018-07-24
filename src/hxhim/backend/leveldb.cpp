#include <ctime>
#include <sstream>
#include <stdexcept>

#include "leveldb/write_batch.h"

#include "hxhim/backend/leveldb.hpp"
#include "hxhim/private.hpp"
#include "hxhim/triplestore.hpp"

static long double elapsed(const struct timespec &start, const struct timespec &end) {
    return (long double) (end.tv_sec - start.tv_sec) +
        ((long double) (end.tv_nsec - start.tv_nsec)/1000000000.0);
}

namespace hxhim {
namespace backend {

leveldb::leveldb(hxhim_t *hx, const std::string &name, const bool create_if_missing)
    : base(hx),
      name(name), create_if_missing(create_if_missing),
      db(nullptr), options(),
      stats()
{
    std::stringstream s;
    s << name << "-" << hx->mpi.rank;

    options.create_if_missing = create_if_missing;

    if (!::leveldb::DB::Open(options, s.str(), &db).ok()) {
        throw std::runtime_error("Could not configure leveldb backend " + s.str());
    }

    memset(&stats, 0, sizeof(stats));
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
 * GetStats
 * Collective operation
 * Collects statistics from all HXHIM ranks
 *
 * @param rank           the rank to send to
 * @param get_put_times  whether or not to get put_times
 * @param put_times      the array of put times from each rank
 * @param get_num_puts   whether or not to get num_puts
 * @param num_puts       the array of number of puts from each rank
 * @param get_get_times  whether or not to get get_times
 * @param get_times      the array of get times from each rank
 * @param get_num_gets   whether or not to get num_gets
 * @param num_gets       the array of number of gets from each rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int leveldb::GetStats(const int rank,
                      const bool get_put_times, long double *put_times,
                      const bool get_num_puts, std::size_t *num_puts,
                      const bool get_get_times, long double *get_times,
                      const bool get_num_gets, std::size_t *num_gets) {
    MPI_Barrier(hx->mpi.comm);

    if (hx->mpi.rank == rank) {
        if (get_put_times) {
            const std::size_t size = sizeof(stats.put_times);
            MPI_Gather(&stats.put_times, size, MPI_CHAR, put_times, size, MPI_CHAR, rank, hx->mpi.comm);
        }

        if (get_num_puts) {
            const std::size_t size = sizeof(stats.puts);
            MPI_Gather(&stats.puts, size, MPI_CHAR, num_puts, size, MPI_CHAR, rank, hx->mpi.comm);
        }

        if (get_get_times) {
            const std::size_t size = sizeof(stats.get_times);
            MPI_Gather(&stats.get_times, size, MPI_CHAR, get_times, size, MPI_CHAR, rank, hx->mpi.comm);
        }

        if (get_num_gets) {
            const std::size_t size = sizeof(stats.gets);
            MPI_Gather(&stats.gets, size, MPI_CHAR, num_gets, size, MPI_CHAR, rank, hx->mpi.comm);
        }
    }
    else {
        if (get_put_times) {
            const std::size_t size = sizeof(stats.put_times);
            MPI_Gather(&stats.put_times, size, MPI_CHAR, nullptr, 0, MPI_CHAR, rank, hx->mpi.comm);
        }

        if (get_num_puts) {
            const std::size_t size = sizeof(stats.puts);
            MPI_Gather(&stats.puts, size, MPI_CHAR, nullptr, 0, MPI_CHAR, rank, hx->mpi.comm);
        }

        if (get_get_times) {
            const std::size_t size = sizeof(stats.get_times);
            MPI_Gather(&stats.get_times, size, MPI_CHAR, nullptr, 0, MPI_CHAR, rank, hx->mpi.comm);
        }

        if (get_num_gets) {
            const std::size_t size = sizeof(stats.gets);
            MPI_Gather(&stats.gets, size, MPI_CHAR, nullptr, 0, MPI_CHAR, rank, hx->mpi.comm);
        }
    }

    MPI_Barrier(hx->mpi.comm);
    return MDHIM_SUCCESS;
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
Results *leveldb::BPut(void **subjects, std::size_t *subject_lens,
                       void **predicates, std::size_t *predicate_lens,
                       void **objects, std::size_t *object_lens,
                       std::size_t count) {
    Results *res = new Results();
    if (!res) {
        return nullptr;
    }

    struct timespec start, end;
    ::leveldb::WriteBatch batch;
    void **keys = new void *[count]();

    for(std::size_t i = 0; i < count; i++) {
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &keys[i], &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        batch.Put(::leveldb::Slice((char *) keys[i], key_len), ::leveldb::Slice((char *) objects[i], object_lens[i]));
        clock_gettime(CLOCK_MONOTONIC, &end);

        stats.puts++;
        stats.put_times += elapsed(start, end);
    }

    // add in the time to write the key-value pairs without adding to the counter
    clock_gettime(CLOCK_MONOTONIC, &start);
    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);
    clock_gettime(CLOCK_MONOTONIC, &end);

    for(std::size_t i = 0; i < count; i++) {
        res->Add(new Results::Put(status.ok()?HXHIM_SUCCESS:HXHIM_ERROR, hx->mpi.rank));
    }

    stats.put_times += elapsed(start, end);

    for(std::size_t i = 0; i < count; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;

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
Results *leveldb::BGet(void **subjects, std::size_t *subject_lens,
                       void **predicates, std::size_t *predicate_lens,
                       std::size_t count) {
    Results *res = new Results();
    if (!res) {
        return nullptr;
    }

    void **keys = new void *[count]();
    for(std::size_t i = 0; i < count; i++) {
        struct timespec start, end;
        std::string value;

        // create the key
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &keys[i], &key_len);
        const ::leveldb::Slice key((char *) keys[i], key_len);

        // get the value
        clock_gettime(CLOCK_MONOTONIC, &start);
        ::leveldb::Status status = db->Get(::leveldb::ReadOptions(), key, &value);
        clock_gettime(CLOCK_MONOTONIC, &end);

        // update stats
        stats.gets++;
        stats.get_times += elapsed(start, end);

        // add to results list
        if (status.ok()) {
            res->Add(new GetResult(&hx->p->types, hx->mpi.rank, key, value));
        }
        else {
            res->Add(new GetResult(&hx->p->types, hx->mpi.rank, key));
        }
    }

    for(std::size_t i = 0; i < count; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;

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
Results *leveldb::BGetOp(void *subject, std::size_t subject_len,
                         void *predicate, std::size_t predicate_len,
                         std::size_t count, enum hxhim_get_op op) {
    Results *res = new Results();
    if (!res) {
        return nullptr;
    }

    // conbine the subject and predicate into a key
    void *starting_key = nullptr;
    std::size_t starting_key_len = 0;
    sp_to_key(subject, subject_len, predicate, predicate_len, &starting_key, &starting_key_len);

    ::leveldb::Iterator *it = db->NewIterator(::leveldb::ReadOptions());
    struct timespec start, end;

    // add in the time to get the first key-value without adding to the counter
    clock_gettime(CLOCK_MONOTONIC, &start);
    it->Seek(::leveldb::Slice((char *) starting_key, starting_key_len));
    clock_gettime(CLOCK_MONOTONIC, &end);

    stats.get_times += elapsed(start, end);

    if (it->status().ok()) {
        for(std::size_t i = 0; i < count && it->Valid(); i++) {
            clock_gettime(CLOCK_MONOTONIC, &start);
            const ::leveldb::Slice key = it->key();
            const ::leveldb::Slice value = it->value();
            clock_gettime(CLOCK_MONOTONIC, &end);

            stats.gets++;
            stats.get_times += elapsed(start, end);

            res->Add(new GetResult(&hx->p->types, hx->mpi.rank, key, value));

            // move to next iterator according to operation
            switch (op) {
                case hxhim_get_op::HXHIM_GET_NEXT:
                    it->Next();
                    break;
                case hxhim_get_op::HXHIM_GET_PREV:
                    it->Prev();
                    break;
                case hxhim_get_op::HXHIM_GET_FIRST:
                    it->Next();
                    break;
                case hxhim_get_op::HXHIM_GET_LAST:
                    it->Prev();
                    break;
                default:
                    break;
            }
        }
    }

    delete it;
    ::operator delete(starting_key);

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
Results *leveldb::BDelete(void **subjects, std::size_t *subject_lens,
                          void **predicates, std::size_t *predicate_lens,
                          std::size_t count) {
    Results *res = new Results();
    ::leveldb::WriteBatch batch;
    void **keys = new void *[count]();

    for(std::size_t i = 0; i < count; i++) {
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &keys[i], &key_len);

        batch.Delete(::leveldb::Slice((char *) keys[i], key_len));
    }

    ::leveldb::Status status = db->Write(::leveldb::WriteOptions(), &batch);
    for(std::size_t i = 0; i < count; i++) {
        res->Add(new Results::Delete(status.ok()?HXHIM_SUCCESS:HXHIM_ERROR, hx->mpi.rank));
    }

    for(std::size_t i = 0; i < count; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;

    return res;
}

std::ostream &leveldb::print_config(std::ostream &stream) const {
    return stream
        << "leveldb" << std::endl
        << "    name: " << name << std::endl
        << "    create_if_missing: " << std::boolalpha << create_if_missing << std::endl;
}

leveldb::GetResult::GetResult(SPO_Types_t *types, const int db, const ::leveldb::Slice &key, const ::leveldb::Slice &value)
    : Get(types, HXHIM_SUCCESS, db),
      k(key.ToString()),
      v(value.ToString())
{}

leveldb::GetResult::GetResult(SPO_Types_t *types, const int db, const ::leveldb::Slice &key, const std::string &value)
    : Get(types, HXHIM_SUCCESS, db),
      k(key.ToString()),
      v(value)
{}

leveldb::GetResult::GetResult(SPO_Types_t *types, const int db, const ::leveldb::Slice &key)
    : Get(types, HXHIM_ERROR, db),
      k(key.ToString()),
      v()
{}

leveldb::GetResult::~GetResult()
{}

int leveldb::GetResult::FillSubject() {
    if (!sub) {
        void *encoded = nullptr;
        std::size_t encoded_len = 0;
        key_to_sp((void *) k.c_str(), k.size(), &encoded, &encoded_len, nullptr, nullptr);
        return decode(types->subject, encoded, encoded_len, &sub, &sub_len);

        // do not delete encoded because it is just an address in another array
    }

    return HXHIM_SUCCESS;
}

int leveldb::GetResult::FillPredicate() {
    if (!pred) {
        void *encoded = nullptr;
        std::size_t encoded_len = 0;
        key_to_sp((void *) k.c_str(), k.size(), nullptr, nullptr, &encoded, &encoded_len);
        return decode(types->predicate, encoded, encoded_len, &pred, &pred_len);

        // do not delete encoded because it is just an address in another array
    }

    return HXHIM_SUCCESS;
}

int leveldb::GetResult::FillObject() {
    if (!obj) {
        return decode(types->object, (void *) v.data(), v.size(), &obj, &obj_len);
    }

    return HXHIM_SUCCESS;
}

}
}
