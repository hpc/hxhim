#include <ctime>
#include <sstream>
#include <stdexcept>

#include "hxhim/backend/InMemory.hpp"
#include "hxhim/private.hpp"
#include "hxhim/triplestore.hpp"

static long double elapsed(const struct timespec &start, const struct timespec &end) {
    return (long double) (end.tv_sec - start.tv_sec) +
        ((long double) (end.tv_nsec - start.tv_nsec)/1000000000.0);
}

namespace hxhim {
namespace backend {

InMemory::InMemory(hxhim_t *hx)
    : base(hx),
      db()
{}

InMemory::~InMemory() {
    Close();
}

void InMemory::Close() {
    db.clear();
}

/**
 * Commit
 * NOOP
 *
 * @return HXHIM_SUCCESS
 */
int InMemory::Commit() {
    return HXHIM_SUCCESS;
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
int InMemory::GetStats(const int rank,
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
Results *InMemory::BPut(void **subjects, std::size_t *subject_lens,
                        void **predicates, std::size_t *predicate_lens,
                        void **objects, std::size_t *object_lens,
                        std::size_t count) {
    Results *res = new Results(hx->p->subject_type, hx->p->predicate_type, hx->p->object_type);
    if (!res) {
        return nullptr;
    }

    struct timespec start, end;
    void **keys = new void *[count]();

    for(std::size_t i = 0; i < count; i++) {
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &keys[i], &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        db[std::string((char *) keys[i], key_len)] = std::string((char *) objects[i], object_lens[i]);
        clock_gettime(CLOCK_MONOTONIC, &end);

        stats.puts++;
        stats.put_times += elapsed(start, end);

        res->AddPut(HXHIM_SUCCESS, hx->mpi.rank);
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
 * Performs a bulk GET in InMemory
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @return pointer to a list of results
 */
Results *InMemory::BGet(void **subjects, std::size_t *subject_lens,
                      void **predicates, std::size_t *predicate_lens,
                      std::size_t count) {
    Results *res = new Results(hx->p->subject_type, hx->p->predicate_type, hx->p->object_type);
    if (!res) {
        return nullptr;
    }

    void **keys = new void *[count]();
    for(std::size_t i = 0; i < count; i++) {
        struct timespec start, end;
        std::string value_str;

        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &keys[i], &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        decltype(db)::const_iterator it = db.find(std::string((char *) keys[i], key_len));
        clock_gettime(CLOCK_MONOTONIC, &end);

        stats.gets++;
        stats.get_times += elapsed(start, end);

        // copy the subject
        void *sub = ::operator new(subject_lens[i]);
        memcpy(sub, subjects[i], subject_lens[i]);

        // copy the predicate
        void *pred = ::operator new(predicate_lens[i]);
        memcpy(pred, predicates[i], predicate_lens[i]);

        // copy the object
        void *obj = nullptr;
        std::size_t obj_len = 0;
        if (it != db.end()){
            obj_len = it->second.size();
            obj = malloc(obj_len);
            memcpy(obj, it->second.c_str(), obj_len);
        }

        res->AddGet((it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR, hx->mpi.rank, sub, subject_lens[i], pred, predicate_lens[i], obj, obj_len);
    }

    for(std::size_t i = 0; i < count; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;

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
Results *InMemory::BGetOp(void *subject, std::size_t subject_len,
                         void *predicate, std::size_t predicate_len,
                         std::size_t count, enum hxhim_get_op op) {
    Results *res = new Results(hx->p->subject_type, hx->p->predicate_type, hx->p->object_type);
    if (!res) {
        return nullptr;
    }

    // conbine the subject and predicate into a key
    void *starting_key = nullptr;
    std::size_t starting_key_len = 0;
    sp_to_key(subject, subject_len, predicate, predicate_len, &starting_key, &starting_key_len);

    struct timespec start, end;

    // add in the time to get the first key-value without adding to the counter
    clock_gettime(CLOCK_MONOTONIC, &start);
    decltype(db)::const_iterator it = db.find(std::string((char *) starting_key, starting_key_len));
    clock_gettime(CLOCK_MONOTONIC, &end);

    decltype(db)::const_reverse_iterator rit = std::make_reverse_iterator(it);

    stats.get_times += elapsed(start, end);

    if (it != db.end()) {
        for(std::size_t i = 0; i < count && (it != db.end()) && (rit != db.rend()); i++) {
            clock_gettime(CLOCK_MONOTONIC, &start);
            const std::string &key = it->first;
            const std::string &value = it->second;
            clock_gettime(CLOCK_MONOTONIC, &end);

            stats.gets++;
            stats.get_times += elapsed(start, end);

            // convert the key into a subject predicate pair
            void *temp_sub = nullptr;
            std::size_t sub_len = 0;
            void *temp_pred = nullptr;
            std::size_t pred_len = 0;

            key_to_sp(key.data(), key.size(), &temp_sub, &sub_len, &temp_pred, &pred_len);

            // copy the subject into a new location
            void *sub = ::operator new(sub_len);
            memcpy(sub, temp_sub, sub_len);

            // copy the predicate into a new location
            void *pred = ::operator new(pred_len);
            memcpy(pred, temp_pred, pred_len);

            // copy the object
            void *obj = nullptr;
            std::size_t obj_len = 0;
            if (it != db.end()){
                obj_len = value.size();
                obj = malloc(obj_len);
                memcpy(obj, value.data(), obj_len);
            }

            res->AddGet(HXHIM_SUCCESS, hx->mpi.rank, sub, sub_len, pred, pred_len, obj, obj_len);

            // move to next iterator according to operation
            switch (op) {
                case hxhim_get_op::HXHIM_GET_NEXT:
                    it++;
                    break;
                case hxhim_get_op::HXHIM_GET_PREV:
                    it--;
                    break;
                case hxhim_get_op::HXHIM_GET_FIRST:
                    it++;
                    break;
                case hxhim_get_op::HXHIM_GET_LAST:
                    it--;
                    break;
                default:
                    break;
            }
        }
    }

    ::operator delete(starting_key);

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
Results *InMemory::BDelete(void **subjects, std::size_t *subject_lens,
                          void **predicates, std::size_t *predicate_lens,
                          std::size_t count) {
    Results *res = new Results(hx->p->subject_type, hx->p->predicate_type, hx->p->object_type);
    void **keys = new void *[count]();

    for(std::size_t i = 0; i < count; i++) {
        std::size_t key_len = 0;
        sp_to_key(subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &keys[i], &key_len);

        db.erase(std::string((char *) keys[i], key_len));
        res->AddPut(HXHIM_SUCCESS, hx->mpi.rank);
    }

    for(std::size_t i = 0; i < count; i++) {
        ::operator delete(keys[i]);
    }
    delete [] keys;

    return res;
}

std::ostream &InMemory::print_config(std::ostream &stream) const {
    return stream;
}

}
}
