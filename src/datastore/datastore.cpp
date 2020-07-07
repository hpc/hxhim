#include <chrono>
#include <cmath>
#include <cstring>
#include <stdexcept>

#include "datastore/datastore.hpp"
#include "hxhim/accessors.hpp"
#include "utils/elen.hpp"
#include "utils/is_range_server.hpp"

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
    std::size_t client = 0;
    std::size_t server = 0;
    if (GetDatastoreClientToServerRatio(hx, &client, &server) != HXHIM_SUCCESS) {
        return -1;
    }

    const std::div_t qr = std::div(get_server(hx, id), server);
    return client * qr.quot + qr.rem;
}

/**
 * get_server
 *
 * @param hx the HXHIM instance
 * @param id the database ID to get the server number of
 * @return the server number of the given ID or -1 on error
 */
int get_server(hxhim_t *hx, const int id) {
    std::size_t local_ds_count = 0;
    if (GetDatastoresPerRangeServer(hx, &local_ds_count) != HXHIM_SUCCESS) {
        return -1;
    }

    return id / local_ds_count;
}

/**
 * get_offset
 *
 * @param hx the HXHIM instance
 * @param id the database ID to get the offset of
 * @return the offset of the given ID or -1 on error
 */
int get_offset(hxhim_t *hx, const int id) {
    std::size_t local_ds_count = 0;
    if (GetDatastoresPerRangeServer(hx, &local_ds_count) != HXHIM_SUCCESS) {
        return -1;
    }

    return id % local_ds_count;
}

/**
 * get_id
 *
 * @param hx      the HXHIM instance
 * @param rank    the destination rank
 * @param offset  the offset within the destination
 * @return the mapping from the (rank, offset) to the database ID, or -1 on error
 */
int get_id(hxhim_t *hx, const int rank, const std::size_t offset) {
    std::size_t local_ds_count = 0;
    if (GetDatastoresPerRangeServer(hx, &local_ds_count) != HXHIM_SUCCESS) {
        return -1;
    }

    if (offset >= local_ds_count) {
        return -1;
    }

    return rank * local_ds_count + offset;
}

Datastore::Datastore(const int rank,
                     const int id,
                     Histogram::Histogram *hist)
    : rank(rank),
      id(id),
      hist(hist),
      mutex(),
      stats()
{}

Datastore::~Datastore() {
    long double put_time = 0;
    for(Stats::Event const &event : stats.puts) {
        put_time += elapsed<std::chrono::nanoseconds>(event.time);
    }
    put_time /= 1e9;

    long double get_time = 0;
    for(Stats::Event const &event : stats.gets) {
        get_time += elapsed<std::chrono::nanoseconds>(event.time);
    }
    get_time /= 1e9;

    std::cerr << "Datastore " << id << ": " << stats.puts.size() << " PUTs in " << put_time << " seconds";
    if (stats.puts.size()) {
        std::cerr << " (" << stats.puts.size() / put_time << " PUTs/sec)";
    }
    std::cerr << std::endl;

    std::cerr << "Datastore " << id << ": " << stats.gets.size() << " GETs in " << get_time << " seconds";
    if (stats.gets.size()) {
        std::cerr << " (" << stats.gets.size() / get_time << " GETs/sec)";
    }
    std::cerr << std::endl;

    delete hist;
}

bool Datastore::Open(const std::string &new_name) {
    Close();
    return OpenImpl(new_name);
}

void Datastore::Close() {
    if (hist) {
        hist->clear();
    }
    CloseImpl();
    return;
}

Transport::Response::BPut *Datastore::operate(Transport::Request::BPut *req) {
    std::lock_guard<std::mutex> lock(mutex);
    Transport::Response::BPut *res = BPutImpl(req);

    if (hist && res) {
        // add successfully PUT floating point values to the histogram
        for(std::size_t i = 0; i < req->count; i++) {
            if (res->statuses[i] == HXHIM_SUCCESS) {
                switch (req->object_types[i]) {
                    case HXHIM_FLOAT_TYPE:
                        hist->add(* (float *) req->objects[i]->data());
                        break;
                    case HXHIM_DOUBLE_TYPE:
                        hist->add(* (double *) req->objects[i]->data());
                        break;
                    default:
                        break;
                }
            }
        }
    }

    return res;
}

Transport::Response::BGet *Datastore::operate(Transport::Request::BGet *req) {
    std::lock_guard<std::mutex> lock(mutex);
    return BGetImpl(req);
}

Transport::Response::BGetOp *Datastore::operate(Transport::Request::BGetOp *req) {
    std::lock_guard<std::mutex> lock(mutex);
    return BGetOpImpl(req);
}

Transport::Response::BDelete *Datastore::operate(Transport::Request::BDelete *req) {
    std::lock_guard<std::mutex> lock(mutex);
    return BDeleteImpl(req);
}

int Datastore::Sync() {
    std::lock_guard<std::mutex> lock(mutex);
    return SyncImpl();
}

/**
 * GetStats
 * Collective operation
 * Collects statistics from all HXHIM ranks.
 * The pointers should be NULL on all ranks that are not the destination
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
int Datastore::GetStats(long double *put_time,
                        std::size_t  *num_put,
                        long double *get_time,
                        std::size_t  *num_get) {
    std::lock_guard<std::mutex> lock(mutex);

    if (put_time) {
        *put_time = 0;
        for(Stats::Event const &event : stats.puts) {
            *put_time += elapsed<std::chrono::nanoseconds>(event.time);
        }
    }

    if (num_put) {
        *num_put = stats.puts.size();
    }

    if (get_time) {
        *get_time = 0;
        for(Stats::Event const &event : stats.gets) {
            *get_time += elapsed<std::chrono::nanoseconds>(event.time);
        }
    }

    if (num_get) {
        *num_get = stats.gets.size();
    }

    return HXHIM_SUCCESS;
}

// /**
//  * Histogram
//  * Puts the histogram data into a transport packet for sending/processing
//  *
//  * @return a pointer to the transport packet containing the histogram data
//  */
// Transport::Response::Histogram *Datastore::Histogram() const {
//     std::lock_guard<std::mutex> lock(mutex);
//     Transport::Response::Histogram *ret = new Transport::Response::Histogram();
//     if (ret) {
//         ret->status = HXHIM_SUCCESS;
//         hist->get(&ret->hist.buckets, &ret->hist.counts, &ret->hist.size);
//     }
//     return ret;
// }

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
