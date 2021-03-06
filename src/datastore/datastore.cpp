#include <cmath>
#include <iomanip>
#include <sstream>

#include "datastore/datastore.hpp"
#include "hxhim/Blob.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/macros.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

const std::string datastore::Datastore::HISTOGRAM_SUBJECT = "HISTOGRAM";

datastore::Datastore::Datastore(const int rank,
                                const int id,
                                Transform::Callbacks *callbacks)
    : rank(rank),
      id(id),
      callbacks(callbacks),
      hists(),
      mutex(),
      stats()
{
    // default to basic callbacks
    if (!this->callbacks) {
        this->callbacks = Transform::default_callbacks();
    }
}

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

    destruct(callbacks);
    hists.clear();

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

bool datastore::Datastore::Open(const std::string &new_name,
                                const datastore::HistNames_t *histogram_names) {
    OpenImpl(new_name);
    if (Usable() && histogram_names) {
        ReadHistogramsImpl(*histogram_names);
    }
    return Usable();
}

void datastore::Datastore::Close(const bool write_histograms) {
    Sync(write_histograms);
    hists.clear();
    CloseImpl();
}

bool datastore::Datastore::Usable() const {
    return UsableImpl();
}

bool datastore::Datastore::Change(const std::string &new_name,
                                  const bool write_histograms,
                                  const datastore::HistNames_t *find_histogram_names) {
    Close(write_histograms);
    return Open(new_name, find_histogram_names);
}

int datastore::Datastore::ID() const {
    return id;
}

Transport::Response::BPut *datastore::Datastore::operate(Transport::Request::BPut *req) {
    std::lock_guard<std::mutex> lock(mutex);
    Transport::Response::BPut *res = Usable()?BPutImpl(req):nullptr;

    if (hists.size() && res) {
        // if a predicate is HXHIM_DATA_BYTE and the PUT was successful
        // keep track of the subject in the histogram

        for(std::size_t i = 0; i < req->count; i++) {
            if (res->statuses[i] == DATASTORE_SUCCESS) {
                switch (req->predicates[i].data_type()) {
                    case HXHIM_DATA_BYTE:
                        {
                            // find the histogram at the provided index
                            REF(hists)::const_iterator hist_it = hists.find((std::string) req->predicates[i]);
                            if (hist_it == hists.end()) {
                                res->statuses[i] = DATASTORE_ERROR;
                            }
                            else {
                                // insert the object
                                switch (req->objects[i].data_type()) {
                                    case HXHIM_DATA_FLOAT:
                                        hist_it->second->add(* (float *) req->objects[i].data());
                                        break;
                                    case HXHIM_DATA_DOUBLE:
                                        hist_it->second->add(* (double *) req->objects[i].data());
                                        break;
                                    default:
                                        break;
                                }
                            }
                        }

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
    return Usable()?BGetImpl(req):nullptr;
}

Transport::Response::BGetOp *datastore::Datastore::operate(Transport::Request::BGetOp *req) {
    std::lock_guard<std::mutex> lock(mutex);
    return Usable()?BGetOpImpl(req):nullptr;
}

Transport::Response::BDelete *datastore::Datastore::operate(Transport::Request::BDelete *req) {
    std::lock_guard<std::mutex> lock(mutex);
    return Usable()?BDeleteImpl(req):nullptr;
}

Transport::Response::BHistogram *datastore::Datastore::operate(Transport::Request::BHistogram *req) {
    /**
     * handle histograms right here because it is datastore agnostic
     */
    std::lock_guard<std::mutex> lock(mutex);

    Datastore::Stats::Event event;
    event.time.start = ::Stats::now();
    event.count = req->count;

    Transport::Response::BHistogram *res = construct<Transport::Response::BHistogram>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        struct timespec start = {};
        struct timespec end = {};

        // find the histogram at the requested index
        clock_gettime(CLOCK_MONOTONIC, &start);
        REF(hists)::const_iterator it = hists.find((std::string) req->names[i]);
        clock_gettime(CLOCK_MONOTONIC, &end);

        if (it != hists.end()) {
            res->histograms[i] = it->second;
            res->statuses[i] = DATASTORE_SUCCESS;
            event.size += res->histograms[i]->pack_size();
        }
        else {
            res->statuses[i] = DATASTORE_ERROR;
        }

        res->count++;
    }

    event.time.end = ::Stats::now();
    stats.gets.emplace_back(event);

    return res;
}

/**
 * WriteHistograms
 * Write histograms into the underlying datastore.
 *
 * @return DATASTORE_SUCCESS or DATASTORE_ERROR
 */
int datastore::Datastore::WriteHistograms() {
    return Usable()?WriteHistogramsImpl():0;
}

/**
 * ReadHistograms
 * Searches for histograms in the underlying datastore
 * that have names from the provided list. Histogram
 * instances that exist are overwritten.
 *
 * @param names  A list of histogram names to look for
 * @return The number of histograms found
 */
std::size_t datastore::Datastore::ReadHistograms(const datastore::HistNames_t &names) {
    return Usable()?ReadHistogramsImpl(names):0;
}

/**
 * AddHistogram
 * Register a histogram to this datastore.
 * Overwrites existing histograms.
 *
 * @param name      the name of the new histogram
 * @param config    the new histogram's configuration
 * @return DATASTORE_SUCCESS
 */
int datastore::Datastore::AddHistogram(const std::string &name, const ::Histogram::Config &config) {
    return AddHistogram(name, construct<::Histogram::Histogram>(config, name));
}

/**
 * AddHistogram
 * Register an existing histogram to this datastore.
 * Ownership is transferred to this datastore.
 *
 * @param name              the name of the new histogram
 * @param new_histogram     a pointer to an existing histogram
 * @return DATASTORE_SUCCESS
 */
int datastore::Datastore::AddHistogram(const std::string &name, ::Histogram::Histogram *new_histogram) {
    if (!new_histogram) {
        return DATASTORE_ERROR;
    }

    hists[name] = std::shared_ptr<::Histogram::Histogram>(new_histogram, ::Histogram::deleter);
    return DATASTORE_SUCCESS;
}

/**
 * GetHistogram
 * Get the pointer to this datatstore's histogram
 *
 * @param name   the name of the histogram to get
 * @param h      A pointer to this histogram pointer
 * @return DATASTORE_SUCCESS if found, else DATASTORE_ERROR
 */
int datastore::Datastore::GetHistogram(const std::string &name, datastore::Datastore::Histogram *h) const {
    if (!h) {
        return DATASTORE_ERROR;
    }

    REF(hists)::const_iterator it = hists.find(name);
    if (it == hists.end()) {
        return DATASTORE_ERROR;
    }

    *h = it->second;

    return DATASTORE_SUCCESS;
}

/**
 * GetHistogram
 * Get the pointer to this datatstore's histogram
 *
 * @param name   the name of the histogram to get
 * @param h      A pointer to this histogram pointer
 * @return DATASTORE_SUCCESS if found, else DATASTORE_ERROR
 */
int datastore::Datastore::GetHistogram(const std::string &name, ::Histogram::Histogram **h) const {
    if (!h) {
        return DATASTORE_ERROR;
    }

    Histogram ds_hist;
    if (GetHistogram(name, &ds_hist) != DATASTORE_SUCCESS) {
        return DATASTORE_ERROR;
    }

    *h = ds_hist.get();

    return DATASTORE_SUCCESS;
}

/**
 * GetHistograms
 * Get a reference to all histograms this datastore holds
 *
 * @param histogtrams  a reference
 * @return DATASTORE_SUCCESS
 */
int datastore::Datastore::GetHistograms(const datastore::Datastore::Histograms **histograms) const {
    if (histograms) {
        *histograms = &hists;
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

int datastore::Datastore::Sync(const bool write_histograms) {
    std::lock_guard<std::mutex> lock(mutex);
    if (write_histograms) {
        WriteHistograms();
    }
    return Usable()?SyncImpl():DATASTORE_ERROR;
}

int datastore::Datastore::encode(Transform::Callbacks *callbacks,
                                 const Blob &src,
                                 void **dst, std::size_t *dst_size) {
    REF(callbacks->encode)::const_iterator it = callbacks->encode.find(src.data_type());
    if (it == callbacks->encode.end()) {
        return DATASTORE_ERROR;
    }

    if (!it->second.first) {
        *dst_size = src.size();
        *dst = alloc(*dst_size);
        memcpy(*dst, src.data(), src.size());
        return DATASTORE_SUCCESS;
    }

    return it->second.first(src.data(), src.size(), dst, dst_size, it->second.second);
}

int datastore::Datastore::decode(Transform::Callbacks *callbacks,
                                 const Blob &src,
                                 void **dst, std::size_t *dst_size) {
    REF(callbacks->decode)::const_iterator it = callbacks->decode.find(src.data_type());
    if (it == callbacks->decode.end()) {
        return DATASTORE_ERROR;
    }

    if (!it->second.first) {
        *dst_size = src.size();
        *dst = alloc(*dst_size);
        memcpy(*dst, src.data(), src.size());
        return DATASTORE_SUCCESS;
    }

    return it->second.first(src.data(), src.size(), dst, dst_size, it->second.second);
}

datastore::Datastore::Stats::Event::Event()
    : time(),
      count(0),
      size(0)
{}

void datastore::Datastore::BGetOp_error_response(Transport::Response::BGetOp *res,
                                                 const std::size_t i,
                                                 Blob &subject, Blob &predicate,
                                                 datastore::Datastore::Stats::Event &event) {
    std::size_t &index = res->num_recs[i];
    res->subjects[i][index]   = std::move(subject);
    res->predicates[i][index] = std::move(predicate);
    index++;

    event.size += subject.pack_size(true) + predicate.pack_size(true);
}
