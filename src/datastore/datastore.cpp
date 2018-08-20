#include <cstring>
#include <stdexcept>

#include "datastore/datastore.hpp"
#include "hxhim/accessors.hpp"
#include "utils/elen.hpp"

namespace hxhim {
namespace datastore {

/**
 * get_rank
 *
 * @param hx the HXHIM instance
 * @param id the database ID to get the rank of
 * @return the rank of the given ID or -1 on error
 */
int get_rank(hxhim_t *hx, const int id) {
    std::size_t ds_count = 0;
    if (hxhim::GetDatastoreCount(hx, &ds_count) != HXHIM_SUCCESS) {
        return -1;
    }

    return id / ds_count;
}

/**
 * get_offset
 *
 * @param hx the HXHIM instance
 * @param id the database ID to get the offset of
 * @return the offset of the given ID or -1 on error
 */
int get_offset(hxhim_t *hx, const int id) {
    std::size_t ds_count = 0;
    if (hxhim::GetDatastoreCount(hx, &ds_count) != HXHIM_SUCCESS) {
        return -1;
    }

    return id % ds_count;
}

/**
 * get_id
 *
 * @param hx   the HXHIM instance
 * @param rank the destination rank
 * @param id   the offset within the destination
 * @return the mapping from the (rank, offset) to the database ID, or -1 on error
 */
int get_id(hxhim_t *hx, const int rank, const int offset) {
    std::size_t ds_count = 0;
    if (hxhim::GetDatastoreCount(hx, &ds_count) != HXHIM_SUCCESS) {
        return -1;
    }

    return rank * ds_count + offset;
}

Datastore::Datastore(hxhim_t *hx,
                     const int id,
                     const std::size_t use_first_n, const HistogramBucketGenerator_t &generator, void *extra_args)
    : hx(hx),
      id(id),
      hist(use_first_n, generator, extra_args),
      mutex(),
      stats()
{}

Datastore::~Datastore() {}

Transport::Response::BPut *Datastore::BPut(void **subjects, std::size_t *subject_lens,
                                           void **predicates, std::size_t *predicate_lens,
                                           hxhim_type_t *object_types, void **objects, std::size_t *object_lens,
                                           std::size_t count) {
    std::lock_guard<std::mutex> lock(mutex);
    Transport::Response::BPut *res = BPutImpl(subjects, subject_lens,
                                              predicates, predicate_lens,
                                              object_types, objects, object_lens,
                                              count);

    // add successfully PUT floating point values to the histogram
    for(std::size_t i = 0; i < count; i++) {
        if (res->statuses[i] == HXHIM_SUCCESS) {
            switch (object_types[i]) {
                case HXHIM_FLOAT_TYPE:
                    hist.add(* (float *) objects[i]);
                    break;
                case HXHIM_DOUBLE_TYPE:
                    hist.add(* (double *) objects[i]);
                    break;
                default:
                    break;
            }
        }
    }

    return res;
}

Transport::Response::BGet *Datastore::BGet(void **subjects, std::size_t *subject_lens,
                                           void **predicates, std::size_t *predicate_lens,
                                           hxhim_type_t *object_types,
                                           std::size_t count) {
    std::lock_guard<std::mutex> lock(mutex);
    return BGetImpl(subjects, subject_lens,
                    predicates, predicate_lens,
                    object_types,
                    count);
}

Transport::Response::BGetOp *Datastore::BGetOp(void *subject, std::size_t subject_len,
                                               void *predicate, std::size_t predicate_len,
                                               hxhim_type_t object_type,
                                               std::size_t recs, enum hxhim_get_op_t op) {
    std::lock_guard<std::mutex> lock(mutex);
    return BGetOpImpl(subject, subject_len,
                      predicate, predicate_len,
                      object_type,
                      recs, op);
}

Transport::Response::BDelete *Datastore::BDelete(void **subjects, std::size_t *subject_lens,
                                                 void **predicates, std::size_t *predicate_lens,
                                                 std::size_t count) {
    std::lock_guard<std::mutex> lock(mutex);
    return BDeleteImpl(subjects, subject_lens,
                       predicates, predicate_lens,
                       count);
}

int Datastore::Sync() {
    std::lock_guard<std::mutex> lock(mutex);
    return SyncImpl();
}

/**
 * GetStats
 * Collective operation
 * Collects statistics from all HXHIM ranks
 *
 * @param dst_rank       the rank to send to
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
int Datastore::GetStats(const int dst_rank,
                        const bool get_put_times, long double *put_times,
                        const bool get_num_puts, std::size_t *num_puts,
                        const bool get_get_times, long double *get_times,
                        const bool get_num_gets, std::size_t *num_gets) {
    MPI_Comm comm = MPI_COMM_NULL;
    if (hxhim::GetMPIComm(hx, &comm) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    int rank = -1;
    if (hxhim::GetMPIRank(hx, &rank) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    MPI_Barrier(comm);

    std::lock_guard<std::mutex> lock(mutex);

    if (rank == dst_rank) {
        if (get_put_times) {
            const std::size_t size = sizeof(stats.put_times);
            MPI_Gather(&stats.put_times, size, MPI_CHAR, put_times, size, MPI_CHAR, dst_rank, comm);
        }

        if (get_num_puts) {
            const std::size_t size = sizeof(stats.puts);
            MPI_Gather(&stats.puts, size, MPI_CHAR, num_puts, size, MPI_CHAR, dst_rank, comm);
        }

        if (get_get_times) {
            const std::size_t size = sizeof(stats.get_times);
            MPI_Gather(&stats.get_times, size, MPI_CHAR, get_times, size, MPI_CHAR, dst_rank, comm);
        }

        if (get_num_gets) {
            const std::size_t size = sizeof(stats.gets);
            MPI_Gather(&stats.gets, size, MPI_CHAR, num_gets, size, MPI_CHAR, dst_rank, comm);
        }
    }
    else {
        if (get_put_times) {
            const std::size_t size = sizeof(stats.put_times);
            MPI_Gather(&stats.put_times, size, MPI_CHAR, nullptr, 0, MPI_CHAR, dst_rank, comm);
        }

        if (get_num_puts) {
            const std::size_t size = sizeof(stats.puts);
            MPI_Gather(&stats.puts, size, MPI_CHAR, nullptr, 0, MPI_CHAR, dst_rank, comm);
        }

        if (get_get_times) {
            const std::size_t size = sizeof(stats.get_times);
            MPI_Gather(&stats.get_times, size, MPI_CHAR, nullptr, 0, MPI_CHAR, dst_rank, comm);
        }

        if (get_num_gets) {
            const std::size_t size = sizeof(stats.gets);
            MPI_Gather(&stats.gets, size, MPI_CHAR, nullptr, 0, MPI_CHAR, dst_rank, comm);
        }
    }

    MPI_Barrier(comm);
    return HXHIM_SUCCESS;
}

/**
 * Histogram
 * Puts the histogram data into a transport packet for sending/processing
 *
 * @return a pointer to the transport packet containing the histogram data
 */
Transport::Response::Histogram *Datastore::Histogram() const {
    std::lock_guard<std::mutex> lock(mutex);
    Transport::Response::Histogram *ret = hxhim::GetResponseFBP(hx)->acquire<Transport::Response::Histogram>(hxhim::GetBufferFBP(hx));
    if (ret) {
        ret->status = HXHIM_SUCCESS;
        hist.get(&ret->hist.buckets, &ret->hist.counts, &ret->hist.size);
    }
    return ret;
}

/**
 * encode
 * Converts the contents of a void * into another
 * format if the type is floating point.
 * This is allowed because the pointers are coming
 * from the internal arrays, and thus can be
 * overwritten/replaced with a new pointer.
 *
 * @param type   the underlying type of this value
 * @param ptr    address of the value
 * @param len    size of the memory being pointed to
 * @param copied whether or not the original ptr was replaced by a copy that needs to be deallocated
 * @param HXHIM_SUCCESS or HXHIM_ERROR
 */
int Datastore::encode(const hxhim_type_t type, void *&ptr, std::size_t &len, bool &copied) {
    if (!ptr) {
        return HXHIM_ERROR;
    }

    switch (type) {
        case HXHIM_FLOAT_TYPE:
            {
                const std::string str = elen::encode::floating_point(* (float *) ptr);
                len = str.size();
                ptr = ::operator new(len);
                memcpy(ptr, str.c_str(), len);
                copied = true;
            }
            break;
        case HXHIM_DOUBLE_TYPE:
            {
                const std::string str = elen::encode::floating_point(* (double *) ptr);
                len = str.size();
                ptr = ::operator new(len);
                memcpy(ptr, str.c_str(), len);
                copied = true;
            }
            break;
        case HXHIM_INT_TYPE:
        case HXHIM_SIZE_TYPE:
        case HXHIM_INT64_TYPE:
        case HXHIM_BYTE_TYPE:
            copied = false;
            break;
        default:
            return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * decode
 * Decodes values taken from a backend back into their
 * local format (i.e. encoded floating point -> double)
 *
 * @param type    the type the data being pointed to by ptr is
 * @param src     the original data
 * @param src_len the original data length
 * @param dst     pointer to the destination buffer of the decoded data
 * @param dst_len pointer to the destination buffer's length
 * @return HXHIM_SUCCESS or HXHIM_ERROR;
 */
int Datastore::decode(const hxhim_type_t type, void *src, const std::size_t &src_len, void **dst, std::size_t *dst_len) {
    if (!src || !dst || !dst_len) {
        return HXHIM_ERROR;
    }

    *dst = nullptr;
    *dst_len = 0;

    // nothing to decode
    if (!src_len) {
        return HXHIM_SUCCESS;
    }

    switch (type) {
        case HXHIM_FLOAT_TYPE:
            {
                const float value = elen::decode::floating_point<float>(std::string((char *) src, src_len));
                *dst_len = sizeof(float);
                *dst = ::operator new(*dst_len);
                memcpy(*dst, &value, *dst_len);
            }
            break;
        case HXHIM_DOUBLE_TYPE:
            {
                const double value = elen::decode::floating_point<double>(std::string((char *) src, src_len));
                *dst_len = sizeof(double);
                *dst = ::operator new(*dst_len);
                memcpy(*dst, &value, *dst_len);
            }
            break;
        case HXHIM_INT_TYPE:
        case HXHIM_SIZE_TYPE:
        case HXHIM_INT64_TYPE:
        case HXHIM_BYTE_TYPE:
            *dst_len = src_len;
            *dst = ::operator new(*dst_len);
            memcpy(*dst, src, *dst_len);
            break;
        default:
            return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

}
}
