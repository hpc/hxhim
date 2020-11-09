#include <cmath>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include "datastore/datastore.hpp"
#include "utils/Blob.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

datastore::Datastore::Datastore(const int rank,
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

datastore::Datastore::~Datastore() {
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

bool datastore::Datastore::Open(const std::string &new_name) {
    Close();
    return OpenImpl(new_name);
}

void datastore::Datastore::Close() {
    CloseImpl();
    return;
}

int datastore::Datastore::ID() const {
    return id;
}

std::size_t datastore::Datastore::all_keys_size(Transport::Request::SubjectPredicate *req) {
    std::size_t total = 0;
    for(std::size_t i = 0; i < req->count; i++) {
        Blob &subject   = req->subjects[i];
        Blob &predicate = req->predicates[i];

        total += subject.pack_size() + predicate.pack_size();
    }

    return total;
}

Transport::Response::BPut *datastore::Datastore::operate(Transport::Request::BPut *req) {
    std::lock_guard<std::mutex> lock(mutex);
    Transport::Response::BPut *res = BPutImpl(req);

    if (hist && res) {
        // add successfully PUT floating point values to the histogram
        for(std::size_t i = 0; i < req->count; i++) {
            if (res->statuses[i] == DATASTORE_SUCCESS) {

                switch (req->object_types[i]) {
                    case HXHIM_OBJECT_TYPE_FLOAT:
                        hist->add(* (float *) req->objects[i].data());
                        break;
                    case HXHIM_OBJECT_TYPE_DOUBLE:
                        hist->add(* (double *) req->objects[i].data());
                        break;
                    default:
                        break;
                }
            }
        }
    }

    return res;
}

Transport::Response::BGet *datastore::Datastore::operate(Transport::Request::BGet *req) {
    std::lock_guard<std::mutex> lock(mutex);
    return BGetImpl(req);
}

Transport::Response::BGetOp *datastore::Datastore::operate(Transport::Request::BGetOp *req) {
    std::lock_guard<std::mutex> lock(mutex);
    return BGetOpImpl(req);
}

Transport::Response::BDelete *datastore::Datastore::operate(Transport::Request::BDelete *req) {
    std::lock_guard<std::mutex> lock(mutex);
    return BDeleteImpl(req);
}

Transport::Response::BHistogram *datastore::Datastore::operate(Transport::Request::BHistogram *req) {
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

        res->statuses[i] = DATASTORE_SUCCESS;

        event.size += res->histograms[i]->pack_size();

        res->count++;
    }

    event.time.end = ::Stats::now();
    stats.gets.emplace_back(event);

    return res;
}

/**
 * Histogram
 * Get the pointer to this datatstore's histogram
 *
 * @param h A pointer to this histogram pointer
 * @return DATASTORE_SUCCESS
 */
int datastore::Datastore::GetHistogram(Histogram::Histogram **h) const {
    if (hist) {
        *h = hist.get();
    }

    return DATASTORE_SUCCESS;
}

/**
 * GetStats
 * Collective operation
 * Collects statistics from all HXHIM ranks.
 * The pointers should be NULL on all ranks that are not the destination
 *
 * @param dst_rank       the rank to send to
 * @param put_times      the array of put times from each rank
 * @param num_puts       the array of number of puts from each rank
 * @param get_times      the array of get times from each rank
 * @param num_gets       the array of number of gets from each rank
 * @return DATASTORE_SUCCESS or DATASTORE_ERROR on error
 */
int datastore::Datastore::GetStats(uint64_t *put_time,
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

    return DATASTORE_SUCCESS;
}

int datastore::Datastore::Sync() {
    std::lock_guard<std::mutex> lock(mutex);
    return SyncImpl();
}

datastore::Datastore::Stats::Event::Event()
    : time(),
      count(0),
      size(0)
{}

void datastore::Datastore::BGetOp_error_response(Transport::Response::BGetOp *res,
                                                        const std::size_t i,
                                                        const Blob &subject, const Blob &predicate,
                                                        datastore::Datastore::Stats::Event &event) {
    res->subjects[i][0]   = ReferenceBlob(subject.data(), subject.size());
    res->predicates[i][0] = ReferenceBlob(predicate.data(), predicate.size());
    res->num_recs[i]++;

    event.size += subject.pack_size() + predicate.pack_size();
}
