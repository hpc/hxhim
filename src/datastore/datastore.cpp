#include <chrono>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "datastore/datastore.hpp"
#include "hxhim/accessors.hpp"
#include "utils/elen.hpp"
#include "utils/is_range_server.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * get_rank
 *
 * @param hx the HXHIM instance
 * @param id the database ID to get the rank of
 * @return the rank of the given ID or -1 on error
 */
int hxhim::datastore::get_rank(hxhim_t *hx, const int id) {
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
int hxhim::datastore::get_server(hxhim_t *hx, const int id) {
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
int hxhim::datastore::get_offset(hxhim_t *hx, const int id) {
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
int hxhim::datastore::get_id(hxhim_t *hx, const int rank, const std::size_t offset) {
    std::size_t local_ds_count = 0;
    if (GetDatastoresPerRangeServer(hx, &local_ds_count) != HXHIM_SUCCESS) {
        return -1;
    }

    if (offset >= local_ds_count) {
        return -1;
    }

    return rank * local_ds_count + offset;
}

hxhim::datastore::Datastore::Datastore(const int rank,
                                       const int id,
                                       Histogram::Histogram *hist)
    : rank(rank),
      id(id),
      hist(std::shared_ptr<Histogram::Histogram>(hist,
                                                 [](Histogram::Histogram *ptr) {
                                                     destruct(ptr);
                                                 })),
      mutex(),
      stats()
{}

static std::string hr_size(const std::size_t size, const long double time) {
    long double rate = size / time;
    static const char *UNITS[] = {"B", "KB", "MB", "GB", "TB", "PB"};
    static const std::size_t UNIT_COUNT = sizeof(UNITS) / sizeof(char *);

    std::size_t i = 0;
    while ((i < UNIT_COUNT) && ((rate / 1024) > 1)) {
        rate /= 1024;
        i++;
    }

    std::stringstream s;
    s << std::fixed << std::setprecision(3) << rate << UNITS[i] << "/sec";
    return s.str();
}

hxhim::datastore::Datastore::~Datastore() {
    mlog(DATASTORE_INFO, "Rank %d Datastore shutting down", rank);

    long double put_time = 0;
    std::size_t put_count = 0;
    std::size_t put_size = 0;
    for(Stats::Event const &event : stats.puts) {
        put_time  += sec(event.time);
        put_count += event.count;
        put_size  += event.size;
    }

    long double get_time = 0;
    std::size_t get_count = 0;
    std::size_t get_size = 0;
    for(Stats::Event const &event : stats.gets) {
        get_time  += sec(event.time);
        get_count += event.count;
        get_size  += event.size;
    }

    if (put_count) {
        mlog(DATASTORE_NOTE, "Rank %d Datastore %d: %zu PUTs (%zu bytes) in %.3Lf seconds (%.3Lf PUTs/sec, %s)", rank, id, put_count, put_size, put_time, put_count / put_time, hr_size(put_size, put_time).c_str());
    }
    else {
        mlog(DATASTORE_NOTE, "Rank %d Datastore %d: %zu PUTs (%zu bytes) in %.3Lf seconds", rank, id, put_count, put_size, put_time);
    }

    if (get_count) {
        mlog(DATASTORE_NOTE, "Rank %d Datastore %d: %zu GETs (%zu bytes) in %.3Lf seconds (%.3Lf GETs/sec, %s)", rank, id, get_count, get_size, get_time, get_count / get_time, hr_size(get_size, get_time).c_str());
    }
    else {
        mlog(DATASTORE_NOTE, "Rank %d Datastore %d: %zu GETs (%zu bytes) in %.3Lf seconds", rank, id, get_count, get_size, get_time);
    }

    mlog(DATASTORE_INFO, "Rank %d Datastore shut down completed", rank);
}

bool hxhim::datastore::Datastore::Open(const std::string &new_name) {
    Close();
    return OpenImpl(new_name);
}

void hxhim::datastore::Datastore::Close() {
    CloseImpl();
    return;
}

int hxhim::datastore::Datastore::ID() const {
    return id;
}

Transport::Response::BPut *hxhim::datastore::Datastore::operate(Transport::Request::BPut *req) {
    std::lock_guard<std::mutex> lock(mutex);
    Transport::Response::BPut *res = BPutImpl(req);

    if (hist && res) {
        // add successfully PUT floating point values to the histogram
        for(std::size_t i = 0; i < req->count; i++) {
            if (res->statuses[i] == HXHIM_SUCCESS) {
                switch (req->object_types[i]) {
                    case HXHIM_OBJECT_TYPE_FLOAT:
                        hist->add(* (float *) req->objects[i]->data());
                        break;
                    case HXHIM_OBJECT_TYPE_DOUBLE:
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

Transport::Response::BGet *hxhim::datastore::Datastore::operate(Transport::Request::BGet *req) {
    std::lock_guard<std::mutex> lock(mutex);
    return BGetImpl(req);
}

Transport::Response::BGetOp *hxhim::datastore::Datastore::operate(Transport::Request::BGetOp *req) {
    std::lock_guard<std::mutex> lock(mutex);
    return BGetOpImpl(req);
}

Transport::Response::BDelete *hxhim::datastore::Datastore::operate(Transport::Request::BDelete *req) {
    std::lock_guard<std::mutex> lock(mutex);
    return BDeleteImpl(req);
}

Transport::Response::BHistogram *hxhim::datastore::Datastore::operate(Transport::Request::BHistogram *req) {
    std::lock_guard<std::mutex> lock(mutex);

    Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BHistogram *res = construct<Transport::Response::BHistogram>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        struct timespec start = {};
        struct timespec end = {};

        clock_gettime(CLOCK_MONOTONIC, &start);
        res->histograms[i] = hist;
        clock_gettime(CLOCK_MONOTONIC, &end);

        res->statuses[i] = HXHIM_SUCCESS;

        event.size += res->histograms[i]->pack_size();

        res->count++;
    }

    event.time.end = ::Stats::now();
    stats.gets.emplace_back(event);

    return res;
}

int hxhim::datastore::Datastore::Sync() {
    std::lock_guard<std::mutex> lock(mutex);
    return SyncImpl();
}

/**
 * Histogram
 * Get the pointer to this datatstore's histogram
 *
 * @param h A pointer to this histogram pointer
 * @return HXHIM_SUCCESS
 */
int hxhim::datastore::Datastore::GetHistogram(Histogram::Histogram **h) const {
    if (hist) {
        *h = hist.get();
    }

    return HXHIM_SUCCESS;
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
int hxhim::datastore::Datastore::GetStats(uint64_t *put_time,
                                          std::size_t  *num_put,
                                          uint64_t *get_time,
                                          std::size_t  *num_get) {
    std::lock_guard<std::mutex> lock(mutex);

    if (put_time) {
        *put_time = 0;
        for(Stats::Event const &event : stats.puts) {
            *put_time += nano(event.time);
        }
    }

    if (num_put) {
        *num_put = stats.puts.size();
    }

    if (get_time) {
        *get_time = 0;
        for(Stats::Event const &event : stats.gets) {
            *get_time += nano(event.time);
        }
    }

    if (num_get) {
        *num_get = stats.gets.size();
    }

    return HXHIM_SUCCESS;
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
int hxhim::datastore::Datastore::encode(const hxhim_object_type_t type, void *&ptr, std::size_t &len, bool &copied) {
    if (!ptr) {
        return HXHIM_ERROR;
    }

    switch (type) {
        case HXHIM_OBJECT_TYPE_FLOAT:
            {
                const std::string str = elen::encode::floating_point(* (float *) ptr);
                len = str.size();
                ptr = ::operator new(len);
                memcpy(ptr, str.c_str(), len);
                copied = true;
            }
            break;
        case HXHIM_OBJECT_TYPE_DOUBLE:
            {
                const std::string str = elen::encode::floating_point(* (double *) ptr);
                len = str.size();
                ptr = ::operator new(len);
                memcpy(ptr, str.c_str(), len);
                copied = true;
            }
            break;
        case HXHIM_OBJECT_TYPE_INT:
        case HXHIM_OBJECT_TYPE_SIZE:
        case HXHIM_OBJECT_TYPE_INT64:
        case HXHIM_OBJECT_TYPE_BYTE:
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
int hxhim::datastore::Datastore::decode(const hxhim_object_type_t type, void *src, const std::size_t &src_len, void **dst, std::size_t *dst_len) {
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
        case HXHIM_OBJECT_TYPE_FLOAT:
            {
                const float value = elen::decode::floating_point<float>(std::string((char *) src, src_len));
                *dst_len = sizeof(float);
                *dst = ::operator new(*dst_len);
                memcpy(*dst, &value, *dst_len);
            }
            break;
        case HXHIM_OBJECT_TYPE_DOUBLE:
            {
                const double value = elen::decode::floating_point<double>(std::string((char *) src, src_len));
                *dst_len = sizeof(double);
                *dst = ::operator new(*dst_len);
                memcpy(*dst, &value, *dst_len);
            }
            break;
        case HXHIM_OBJECT_TYPE_INT:
        case HXHIM_OBJECT_TYPE_SIZE:
        case HXHIM_OBJECT_TYPE_INT64:
        case HXHIM_OBJECT_TYPE_BYTE:
            *dst_len = src_len;
            *dst = ::operator new(*dst_len);
            memcpy(*dst, src, *dst_len);
            break;
        default:
            return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

hxhim::datastore::Datastore::Stats::Event::Event()
    : time(),
      count(0),
      size(0)
{}
