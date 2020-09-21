#include <algorithm>
#include <cmath>
#include <cstring>
#include <functional>
#include <memory>

#include "hxhim/hxhim.hpp"
#include "hxhim/private/Results.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "hxhim/private/process.hpp"
#include "transport/transports.hpp"
#include "utils/is_range_server.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

const char *HXHIM_PUT_COMBINATION_STR[] = {
    HXHIM_PUT_COMBINATION_GEN(HXHIM_PUT_COMBINATION_PREFIX, GENERATE_STR)
};

const HXHIM_PUT_COMBINATION HXHIM_PUT_COMBINATIONS_ENABLED[] = {
    HXHIM_PUT_COMBINATION_SPO,

    #if SOP
    HXHIM_PUT_COMBINATION_SOP,
    #endif

    #if PSO
    HXHIM_PUT_COMBINATION_PSO,
    #endif

    #if POS
    HXHIM_PUT_COMBINATION_POS,
    #endif

    #if OSP
    HXHIM_PUT_COMBINATION_OSP,
    #endif

    #if OPS
    HXHIM_PUT_COMBINATION_OPS,
    #endif
};

int hxhim_put_combination_enabled(const HXHIM_PUT_COMBINATION combo) {
    for(std::size_t i = 0; i < HXHIM_PUT_MULTIPLIER; i++) {
        if (HXHIM_PUT_COMBINATIONS_ENABLED[i] == combo) {
            return 1;
        }
    }
    return 0;
};

const size_t HXHIM_PUT_MULTIPLIER = sizeof(HXHIM_PUT_COMBINATIONS_ENABLED) / sizeof(HXHIM_PUT_COMBINATION_SPO);

const char *HXHIM_OP_STR[] = {
    HXHIM_OP_GEN(HXHIM_OP_PREFIX, GENERATE_STR)
};

const char *HXHIM_GETOP_STR[] = {
    HXHIM_GETOP_GEN(HXHIM_GETOP_PREFIX, GENERATE_STR)
};

const char *HXHIM_OBJECT_TYPE_STR[] = {
    HXHIM_OBJECT_TYPE_GEN(HXHIM_OBJECT_TYPE_PREFIX, GENERATE_STR)
};

/**
 * Open
 * Start a HXHIM session
 *
 * @param hx   the HXHIM session
 * @param opts the HXHIM options to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Open(hxhim_t *hx, hxhim_options_t *opts) {
    if (!hx || !valid(opts)) {
        return HXHIM_ERROR;
    }

    #if PRINT_TIMESTAMPS
    ::Stats::Chronostamp init;
    init.start = ::Stats::now();
    #endif

    //Open mlog - stolen from plfs
    mlog_open((char *) "hxhim", 0, opts->p->debug_level, opts->p->debug_level, nullptr, 0, MLOG_LOGPID, 0);

    mlog(HXHIM_CLIENT_INFO, "Initializing HXHIM");

    hx->p = new hxhim_private_t();

    if ((init::bootstrap(hx, opts) != HXHIM_SUCCESS) ||
        (init::running  (hx, opts) != HXHIM_SUCCESS) ||
        (init::memory   (hx, opts) != HXHIM_SUCCESS) ||
        (init::hash     (hx, opts) != HXHIM_SUCCESS) ||
        (init::datastore(hx, opts) != HXHIM_SUCCESS) ||
        (init::async_put(hx, opts) != HXHIM_SUCCESS) ||
        (init::transport(hx, opts) != HXHIM_SUCCESS)) {
        MPI_Barrier(hx->p->bootstrap.comm);
        Close(hx);
        mlog(HXHIM_CLIENT_ERR, "Failed to initialize HXHIM");
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_INFO, "Waiting for everyone to complete initialization");
    MPI_Barrier(hx->p->bootstrap.comm);
    mlog(HXHIM_CLIENT_INFO, "Successfully initialized HXHIM on rank %d/%d", hx->p->bootstrap.rank, hx->p->bootstrap.size);
    #if PRINT_TIMESTAMPS
    init.end = ::Stats::now();
    ::Stats::print_event(std::cerr, hx->p->bootstrap.rank, "Open",      ::Stats::global_epoch, init);
    #endif
    return HXHIM_SUCCESS;
}

/**
 * hxhimOpen
 * Start a HXHIM session
 *
 * @param hx             the HXHIM session
 * @param bootstrap_comm the MPI communicator used to boostrap MDHIM
 * @param filename       the name of the file to open (for now, it is only the mdhim configuration)
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimOpen(hxhim_t *hx, hxhim_options_t *opts) {
    return hxhim::Open(hx, opts);
}

/**
 * OpenOne
 * Starts a HXHIM session with only 1 backend datastore.
 * This can only be called when the world size is 1.
 *
 * @param hx       the HXHIM session
 * @param opts     the HXHIM options to use
 * @param db_path  the name of the datastore to pass
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::OpenOne(hxhim_t *hx, hxhim_options_t *opts, const std::string &db_path) {
    if (!hx || !valid(opts)) {
        return HXHIM_ERROR;
    }

    //Open mlog - stolen from plfs
    mlog_open((char *) "hxhim", 0, opts->p->debug_level, opts->p->debug_level, nullptr, 0, MLOG_LOGPID, 0);

    hx->p = new hxhim_private_t();

    if ((init::bootstrap     (hx, opts)          != HXHIM_SUCCESS) ||
        (hx->p->bootstrap.size                   != 1)             ||    // Only allow for 1 rank
        (init::running       (hx, opts)          != HXHIM_SUCCESS) ||
        (init::memory        (hx, opts)          != HXHIM_SUCCESS) ||
        // (init::hash          (hx, opts)          != HXHIM_SUCCESS) || // hash is ignored
        (init::one_datastore (hx, opts, db_path) != HXHIM_SUCCESS) ||
        (init::async_put     (hx, opts)          != HXHIM_SUCCESS)) {
        MPI_Barrier(hx->p->bootstrap.comm);
        Close(hx);
        mlog(HXHIM_CLIENT_ERR, "Failed to initialize HXHIM");
        return HXHIM_ERROR;
    }

    mlog(HXHIM_CLIENT_INFO, "Successfully initialized HXHIM");
    MPI_Barrier(hx->p->bootstrap.comm);
    return HXHIM_SUCCESS;
}

/**
 * OpenOne
 * Starts a HXHIM session with only 1 backend datastore
 *
 * @param hx       the HXHIM session
 * @param opts     the HXHIM options to use
 * @param db_path  the name of the datastore to pass
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimOpenOne(hxhim_t *hx, hxhim_options_t *opts, const char *db_path, const size_t db_path_len) {
    return hxhim::OpenOne(hx, opts, std::string(db_path, db_path_len));
}

/**
 * Close
 * Terminates a HXHIM session
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Close(hxhim_t *hx) {
    if (!valid(hx)) {
        mlog(HXHIM_CLIENT_ERR, "Bad HXHIM instance");
        return HXHIM_ERROR;
    }

    MPI_Comm comm = MPI_COMM_NULL;
    int rank = -1;
    int size = -1;
    hxhim::nocheck::GetMPI(hx, &comm, &rank, &size);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Starting to shutdown HXHIM", rank);

    mlog(HXHIM_CLIENT_DBG, "Rank %d No longer accepting user input", rank);
    destroy::running(hx);

    mlog(HXHIM_CLIENT_DBG, "Rank %d Waiting for all ranks to complete syncing", rank);
    Results::Destroy(hxhim::Sync(hx));
    MPI_Barrier(comm);
    mlog(HXHIM_CLIENT_DBG, "Rank %d Closing HXHIM", rank);

    destroy::async_put(hx);
    destroy::transport(hx);
    destroy::hash(hx);
    destroy::datastore(hx);
    destroy::memory(hx);

    // stats should not be modified any more

    // print stats here for now
    for(int i = 0; i < size; i++) {
        MPI_Barrier(comm);
        if (rank == i) {
            std::stringstream s;
            print_stats(hx, s);
            mlog(HXHIM_CLIENT_NOTE, "Rank %d Stats\n%s", rank, s.str().c_str());
        }
    }

    destroy::bootstrap(hx);

    // clean up pointer to private data
    delete hx->p;
    hx->p = nullptr;

    mlog(HXHIM_CLIENT_INFO, "Rank %d HXHIM has been shutdown", rank);
    mlog_close();
    return HXHIM_SUCCESS;
}

/**
 * hxhimClose
 * Terminates a HXHIM session
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimClose(hxhim_t *hx) {
    return hxhim::Close(hx);
}

/**
 * Flush
 * Generic flush function
 * Converts a UserData_t into Request_ts for transport
 * The Request_ts are converted to Response_ts upon completion of the operation and returned
 *
 * @tparam UserData_t      unsorted hxhim user data
 * @tparam Request_t       the transport request type
 * @tparam Response_t      the transport response type
 * @param hx               the HXHIM session
 * @param unsent           queue of unsent requests
 * @param max_ops_per_send maximum operations per send
 * @return results of flushing the queue
 */
template <typename UserData_t, typename Request_t, typename Response_t>
hxhim::Results *FlushImpl(hxhim_t *hx,
                          hxhim::Unsent<UserData_t> &unsent,
                          const std::size_t max_ops_per_send) {
    return hxhim::process<UserData_t, Request_t, Response_t>(hx, unsent.take(), max_ops_per_send);
}

/**
 * FlushPuts
 * Flushes all queued PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return results from sending the PUTs
 */
hxhim::Results *hxhim::FlushPuts(hxhim_t *hx) {
    if (!hxhim::valid(hx)) {
        return nullptr;
    }

    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Flushing PUTs", rank);

    hxhim::Results *res = nullptr;

    // if there are PUT results from the background thread
    // use that as the return pointer
    {
        // If background thread is running, this
        // will block until the thread finishes.
        std::unique_lock<std::mutex> lock(hx->p->async_put.mutex);
        if (hx->p->async_put.results) {
            res = hx->p->async_put.results;
            hx->p->async_put.results = nullptr;
        }
    }

    // append new results to old results
    hxhim::Results *put_results = FlushImpl<hxhim::PutData, Transport::Request::BPut, Transport::Response::BPut>(hx, hx->p->queues.puts, hx->p->max_ops_per_send);
    if (res) {
        res->Append(put_results);
        hxhim::Results::Destroy(put_results);
    }
    else {
        res = put_results;
    }

    mlog(HXHIM_CLIENT_INFO, "Rank %d Done Flushing Puts", rank);
    return res;
}

/**
 * hxhimFlushPuts
 * Flushes all queued PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushPuts(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::FlushPuts(hx));
}

/**
 * FlushGets
 * Flushes all queued GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushGets(hxhim_t *hx) {
    if (!hxhim::valid(hx)) {
        return nullptr;
    }

    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Flushing GETs", rank);
    hxhim::Results *res = FlushImpl<hxhim::GetData, Transport::Request::BGet, Transport::Response::BGet>(hx, hx->p->queues.gets, hx->p->max_ops_per_send);
    mlog(HXHIM_CLIENT_INFO, "Rank %d Done Flushing Gets %p", rank, res);
    return res;
}

/**
 * hxhimFlushGets
 * Flushes all queued GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushGets(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::FlushGets(hx));
}

/**
 * FlushGetOps
 * Flushes all queued GETs with specific operations
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushGetOps(hxhim_t *hx) {
    if (!hxhim::valid(hx)) {
        return nullptr;
    }

    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Flushing GETOPs", rank);
    hxhim::Results *res = FlushImpl<hxhim::GetOpData, Transport::Request::BGetOp, Transport::Response::BGetOp>(hx, hx->p->queues.getops, hx->p->max_ops_per_send);
    mlog(HXHIM_CLIENT_INFO, "Rank %d Done Flushing GETOPs %p", rank, res);
    return res;
}

/**
 * hxhimFlushGetOps
 * Flushes all queued GETs with specific operations
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushGetOps(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::FlushGetOps(hx));
}

/**
 * FlushDeletes
 * Flushes all queued DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushDeletes(hxhim_t *hx) {
    if (!hxhim::valid(hx)) {
        return nullptr;
    }

    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Flushing DELETEs", rank);
    hxhim::Results *res = FlushImpl<hxhim::DeleteData, Transport::Request::BDelete, Transport::Response::BDelete>(hx, hx->p->queues.deletes, hx->p->max_ops_per_send);
    mlog(HXHIM_CLIENT_INFO, "Rank %d Done Flushing DELETEs", rank);
    return res;
}

/**
 * hxhimFlushDeletes
 * Flushes all queued DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushDeletes(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::FlushDeletes(hx));
}

/**
 * FlushHistograms
 * Flushes all queued HISTOGRAMSs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushHistograms(hxhim_t *hx) {
    if (!hxhim::valid(hx)) {
        return nullptr;
    }

    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Flushing HISTOGRAMs", rank);
    hxhim::Results *res = FlushImpl<hxhim::HistogramData, Transport::Request::BHistogram, Transport::Response::BHistogram>(hx, hx->p->queues.histograms, hx->p->max_ops_per_send);
    mlog(HXHIM_CLIENT_INFO, "Rank %d Done Flushing HISTOGRAMs", rank);
    return res;
}

/**
 * hxhimFlushHistograms
 * Flushes all queued HISTOGRAMs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushHistograms(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::FlushHistograms(hx));
}

/**
 * Flush
 *     1. Do all PUTs
 *     2. Do all GETs
 *     3. Do all GET_OPs
 *     4. Do all DELs
 *     5. Do all HISTOGRAMs
 *
 * @param hx the HXHIM session
 * @return A list of results
 */
hxhim::Results *hxhim::Flush(hxhim_t *hx) {
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Flushing", rank);
    hxhim::Results *res    = construct<hxhim::Results>(hx);

    hxhim::Results *puts   = FlushPuts(hx);
    res->Append(puts);     destruct(puts);

    hxhim::Results *gets   = FlushGets(hx);
    res->Append(gets);     destruct(gets);

    hxhim::Results *getops = FlushGetOps(hx);
    res->Append(getops);   destruct(getops);

    hxhim::Results *dels   = FlushDeletes(hx);
    res->Append(dels);     destruct(dels);

    hxhim::Results *hists  = FlushHistograms(hx);
    res->Append(hists);    destruct(hists);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Completed Flushing", rank);
    return res;
}

/**
 * hxhimFlush
 * Push all queued work into MDHIM
 *
 * @param hx
 * @return An array of results (3 values)
 */
hxhim_results_t *hxhimFlush(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::Flush(hx));
}

/**
 * Sync
 * Collective operation
 * Force all queues to be emptied out and
 * writes all data to the backing storage.
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
hxhim::Results *hxhim::Sync(hxhim_t *hx) {
    if (!hxhim::valid(hx)) {
        return nullptr;
    }

    hxhim::Results *res = Flush(hx);

    ::Stats::Send send;
    send.cached.start = ::Stats::now();
    send.cached.end   = send.cached.start;
    send.shuffled     = ::Stats::now();
    send.hashed.start = ::Stats::now();
    send.hashed.end   = send.hashed.end;
    send.bulked.start = ::Stats::now();
    send.bulked.end   = send.bulked.end;

    std::shared_ptr<struct ::Stats::SendRecv> transport = std::make_shared<struct ::Stats::SendRecv>();

    transport->pack.start = ::Stats::now();
    transport->pack.end   = ::Stats::now();

    transport->send_start = ::Stats::now();
    MPI_Barrier(hx->p->bootstrap.comm);
    transport->recv_end = ::Stats::now();

    transport->unpack.start = ::Stats::now();
    transport->unpack.end   = ::Stats::now();

    // Sync local data store
    for(std::size_t i = 0; i < hx->p->datastores.size(); i++) {
        transport->start = ::Stats::now();

        const int synced = hx->p->datastores[i]->Sync();
        hxhim::Results::Sync *sync = hxhim::Result::init(hx, i, synced);

        sync->timestamps.send = construct<::Stats::Send>(send);
        sync->timestamps.transport = transport;
        sync->timestamps.transport->end = ::Stats::now();
        sync->timestamps.recv.result.start = ::Stats::now();
        res->Add(sync);
        sync->timestamps.recv.result.end = ::Stats::now();
    }

    return res;
}

/**
 * hxhimSync
 * Force all queues to be emptied out and
 * writes all data to the backing storage.
 *
 * @param hx the HXHIM session
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
hxhim_results_t *hxhimSync(hxhim_t *hx) {
    return hxhim_results_init(hx, hxhim::Sync(hx));
}

/**
 * ChangeHash
 * Changes the hash function and associated
 * datastores.
 *     - This function is a collective.
 *     - Previously queued operations are flushed
 *     - The datastores are synced before the hash is switched.
 *
 * @param hx   the HXHIM session
 * @param name the name of the new hash function
 * @param func the new hash function
 * @param args the extra args that are used in the hash function
 * @return A list of results
 */
hxhim::Results *hxhim::ChangeHash(hxhim_t *hx, const char *name, hxhim_hash_t func, void *args) {
    if (!hxhim::valid(hx)) {
        return nullptr;
    }

    hxhim::Results *res = Sync(hx);

    MPI_Barrier(hx->p->bootstrap.comm);

    // change hashes
    hx->p->hash.func = func;
    hx->p->hash.args = args;

    // change datastores
    for(std::size_t i = 0; i < hx->p->datastores.size(); i++) {
        std::stringstream s;
        s << name << "-" << hxhim::datastore::get_id(hx, hx->p->bootstrap.rank, i);
        hx->p->datastores[i]->Open(s.str());
    }

    MPI_Barrier(hx->p->bootstrap.comm);

    return res;
}

/**
 * ChangeHash
 * Changes the hash function and associated
 * datastores.
 *     - This function is a collective.
 *     - Previously queued operations are flushed
 *     - The datastores are synced before the hash is switched.
 *
 * @param hx   the HXHIM session
 * @param name the name of the new hash function
 * @param func the new hash function
 * @param args the extra args that are used in the hash function
 * @return A list of results
 */
hxhim_results_t *hxhimChangeHash(hxhim_t *hx, const char *name, hxhim_hash_t func, void *args) {
    return hxhim_results_init(hx, hxhim::ChangeHash(hx, name, func, args));
}

#if !ASYNC_PUTS
/**
 * serial_puts
 * This is effectively FlushPuts but runs in hxhim::Put and hxhim::BPut
 * to simulate background PUTs when threading is not allowed. The results
 * are placed into the background PUTs results buffer.
 *
 * @param hx the HXHIM session
 */
static void serial_puts(hxhim_t *hx) {
    if (hx->p->queues.puts.count >= hx->p->async_put.max_queued) {
        // don't call FlushPuts to avoid deallocating old Results only to allocate a new one
        hxhim::Results *res = hxhim::process<hxhim::PutData, Transport::Request::BPut, Transport::Response::BPut>(hx, hx->p->queues.puts.take(), hx->p->max_ops_per_send);

        {
            std::unique_lock<std::mutex> lock(hx->p->async_put.mutex);
            if (hx->p->async_put.results) {
                hx->p->async_put.results->Append(res);
                hxhim::Results::Destroy(res);
            }
            else {
                hx->p->async_put.results = res;
            }
        }
    }

    hx->p->queues.puts.done_processing.notify_all();
}
#endif

/**
 * Put
 * Add a PUT into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @param object         the object to put
 * @param object_len     the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Put(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len,
               enum hxhim_object_type_t object_type, void *object, std::size_t object_len) {
    mlog(HXHIM_CLIENT_DBG, "%s %s:%d", __FILE__, __func__, __LINE__);
    if (!valid(hx) || !hx->p->running) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp put;
    put.start = ::Stats::now();

    const int rc = hxhim::PutImpl(hx->p->queues.puts,
                                  ReferenceBlob(subject, subject_len),
                                  ReferenceBlob(predicate, predicate_len),
                                  object_type,
                                  ReferenceBlob(object, object_len));

    #if ASYNC_PUTS
    hx->p->queues.puts.start_processing.notify_all();
    #else
    serial_puts(hx);
    #endif

    put.end = ::Stats::now();
    hx->p->stats.single_op[hxhim_op_t::HXHIM_PUT].emplace_back(put);
    return rc;
}

/**
 * hxhimPut
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object_type  the type of the object
 * @param object       the object to put
 * @param object_len   the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimPut(hxhim_t *hx,
             void *subject, size_t subject_len,
             void *predicate, size_t predicate_len,
             enum hxhim_object_type_t object_type, void *object, size_t object_len) {
    mlog(HXHIM_CLIENT_DBG, "%s %s:%d", __FILE__, __func__, __LINE__);
    return hxhim::Put(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      object_type, object, object_len);
}

/**
 * Get
 * Add a GET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @param object         the prediate to put
 * @param object_len     the length of the prediate to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Get(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len,
               enum hxhim_object_type_t object_type) {
    if (!valid(hx) || !hx->p->running) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp get;
    get.start = ::Stats::now();
    const int rc = hxhim::GetImpl(hx->p->queues.gets,
                                  ReferenceBlob(subject, subject_len),
                                  ReferenceBlob(predicate, predicate_len),
                                  object_type);
    get.end = ::Stats::now();
    hx->p->stats.single_op[hxhim_op_t::HXHIM_GET].emplace_back(get);
    return rc;
}

/**
 * Get
 * Add a GET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @param object_type    the type of the object
 * @param object         the prediate to put
 * @param object_len     the length of the prediate to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGet(hxhim_t *hx,
             void *subject, size_t subject_len,
             void *predicate, size_t predicate_len,
             enum hxhim_object_type_t object_type) {
    return hxhim::Get(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      object_type);
}

/**
 * GetOp
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subjects to get
 * @param subject_len    the lengths of the subjects to get
 * @param predicate      the predicates to get
 * @param predicate_len  the lengths of the predicates to get
 * @param object_type    the type of the object
 * @param num_records    maximum number of records to GET
 * @param op             the operation to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetOp(hxhim_t *hx,
                 void *subject, std::size_t subject_len,
                 void *predicate, std::size_t predicate_len,
                 enum hxhim_object_type_t object_type,
                 std::size_t num_records, enum hxhim_getop_t op) {
    if (!valid(hx) || !hx->p->running ||
        !subject   || !subject_len    ||
        !predicate || !predicate_len)  {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp bgetop;
    bgetop.start = ::Stats::now();
    const int rc = hxhim::GetOpImpl(hx->p->queues.getops,
                                    ReferenceBlob(subject, subject_len),
                                    ReferenceBlob(predicate, predicate_len),
                                    object_type,
                                    num_records, op);
    bgetop.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_GETOP].emplace_back(bgetop);
    return rc;
}

/**
 * hxhimGetOp
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subjects to get
 * @param subject_len    the lengths of the subjects to get
 * @param predicate      the predicates to get
 * @param predicate_len  the lengths of the predicates to get
 * @param object_type    the type of the object
 * @param num_records    maximum number of records to GET
 * @param op             the operation to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetOp(hxhim_t *hx,
                 void *subject, size_t subject_len,
                 void *predicate, size_t predicate_len,
                 enum hxhim_object_type_t object_type,
                 std::size_t num_records, enum hxhim_getop_t op) {
    return hxhim::GetOp(hx,
                        subject, subject_len,
                        predicate, predicate_len,
                        object_type,
                        num_records, op);
}

/**
 * Delete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Delete(hxhim_t *hx,
                  void *subject, std::size_t subject_len,
                  void *predicate, std::size_t predicate_len) {
    if (!valid(hx) || !hx->p->running) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp del;
    del.start = ::Stats::now();
    const int rc = hxhim::DeleteImpl(hx->p->queues.deletes,
                                     ReferenceBlob(subject, subject_len),
                                     ReferenceBlob(predicate, predicate_len));
    del.end = ::Stats::now();
    hx->p->stats.single_op[hxhim_op_t::HXHIM_DELETE].emplace_back(del);
    return rc;
}

/**
 * hxhimDelete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimDelete(hxhim_t *hx,
                void *subject, size_t subject_len,
                void *predicate, size_t predicate_len) {
    return hxhim::Delete(hx,
                         subject, subject_len,
                         predicate, predicate_len);
}

/**
 * BPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param object_type   the type of the object
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects to put
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BPut(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                enum hxhim_object_type_t *object_types, void **objects, std::size_t *object_lens,
                const std::size_t count) {
    if (!valid(hx)  || !hx->p->running ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !objects    || !object_lens) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp bput;
    bput.start = ::Stats::now();

    // append these spo triples into the list of unsent PUTs
    for(std::size_t i = 0; i < count; i++) {
        hxhim::PutImpl(hx->p->queues.puts,
                       ReferenceBlob(subjects[i], subject_lens[i]),
                       ReferenceBlob(predicates[i], predicate_lens[i]),
                       object_types[i],
                       ReferenceBlob(objects[i], object_lens[i]));

    }

    #if ASYNC_PUTS
    hx->p->queues.puts.start_processing.notify_all();
    #else
    serial_puts(hx);
    #endif

    bput.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_PUT].emplace_back(bput);
    return HXHIM_SUCCESS;
}

/**
 * hxhimBPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param object_types  the type of the object
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects to put
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBPut(hxhim_t *hx,
              void **subjects, size_t *subject_lens,
              void **predicates, size_t *predicate_lens,
              enum hxhim_object_type_t *object_types, void **objects, size_t *object_lens,
              const size_t count) {
    return hxhim::BPut(hx,
                       subjects, subject_lens,
                       predicates, predicate_lens,
                       object_types, objects, object_lens,
                       count);
}

/**
 * BGet
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the prediates to get
 * @param predicate_lens the lengths of the prediates to get
 * @param object_types   the types of the objects
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGet(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                hxhim_object_type_t *object_types,
                const std::size_t count) {
    if (!valid(hx)  || !hx->p->running ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !object_types)                  {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp bget;
    bget.start = ::Stats::now();
    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetImpl(hx->p->queues.gets,
                       ReferenceBlob(subjects[i], subject_lens[i]),
                       ReferenceBlob(predicates[i], predicate_lens[i]),
                       object_types[i]);
    }
    bget.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_GET].emplace_back();
    return HXHIM_SUCCESS;
}

/**
 * hxhimBGet
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the prediates to get
 * @param predicate_lens the lengths of the prediates to get
 * @param object_types   the types of the objects
 * @param objects        the prediates to get
 * @param object_lens    the lengths of the prediates to get
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGet(hxhim_t *hx,
              void **subjects, size_t *subject_lens,
              void **predicates, size_t *predicate_lens,
              enum hxhim_object_type_t *object_types,
              const std::size_t count) {
    return hxhim::BGet(hx,
                       subjects, subject_lens,
                       predicates, predicate_lens,
                       object_types,
                       count);
}

/**
 * BGetOp
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the predicates to get
 * @param predicate_lens the lengths of the predicates to get
 * @param object_types   the type of the objects
 * @param num_records    maximum numbers of records to GET
 * @param op             the operations to use
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGetOp(hxhim_t *hx,
                  void **subjects, std::size_t *subject_lens,
                  void **predicates, std::size_t *predicate_lens,
                  enum hxhim_object_type_t *object_types,
                  std::size_t *num_records, enum hxhim_getop_t *ops,
                  const std::size_t count) {
    if (!valid(hx)    || !hx->p->running  ||
        !subjects     || !subject_lens    ||
        !predicates   || !predicate_lens  ||
        !object_types ||
        !num_records  || !ops)             {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp bgetop;
    bgetop.start = ::Stats::now();
    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetOpImpl(hx->p->queues.getops,
                         ReferenceBlob(subjects[i], subject_lens[i]),
                         ReferenceBlob(predicates[i], predicate_lens[i]),
                         object_types[i],
                         num_records[i], ops[i]);
    }
    bgetop.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_GETOP].emplace_back();
    return HXHIM_SUCCESS;
}

/**
 * hxhimBGetOp
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the predicates to get
 * @param predicate_lens the lengths of the predicates to get
 * @param object_types   the type of the objects
 * @param num_records    maximum numbers of records to GET
 * @param op             the operations to use
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGetOp(hxhim_t *hx,
                void **subjects, size_t *subject_lens,
                void **predicates, size_t *predicate_lens,
                enum hxhim_object_type_t *object_types,
                size_t *num_records, enum hxhim_getop_t *ops,
                const size_t count) {
    return hxhim::BGetOp(hx,
                         subjects, subject_lens,
                         predicates, predicate_lens,
                         object_types,
                         num_records, ops,
                         count);
}

/**
 * BDelete
 * Add a BDEL into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to delete
 * @param subject_lens  the lengths of the subjects to delete
 * @param prediates     the prediates to delete
 * @param prediate_lens the lengths of the prediates to delete
 * @param object_type   the type of the object
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BDelete(hxhim_t *hx,
                   void **subjects, std::size_t *subject_lens,
                   void **predicates, std::size_t *predicate_lens,
                   const std::size_t count) {
    if (!valid(hx)  || !hx->p->running ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp bdel;
    bdel.start = ::Stats::now();
    for(std::size_t i = 0; i < count; i++) {
        hxhim::DeleteImpl(hx->p->queues.deletes,
                          ReferenceBlob(subjects[i], subject_lens[i]),
                          ReferenceBlob(predicates[i], predicate_lens[i]));
    }

    bdel.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_DELETE].emplace_back(bdel);
    return HXHIM_SUCCESS;
}

/**
 * hxhimBDelete
 * Add a BDEL into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to delete
 * @param subject_lens  the lengths of the subjects to delete
 * @param prediates     the prediates to delete
 * @param prediate_lens the lengths of the prediates to delete
 * @param object_type   the type of the object
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBDelete(hxhim_t *hx,
                 void **subjects, size_t *subject_lens,
                 void **predicates, size_t *predicate_lens,
                 const size_t count) {
    return hxhim::BDelete(hx,
                          subjects, subject_lens,
                          predicates, predicate_lens,
                          count);
}

/**
 * GetStats
 * Collective operation
 * Each desired pointer should be preallocated with space for md->size values
 *
 * @param hx             the HXHIM session
 * @param dst_rank       the rank that is collecting the data
 * @param put_times      the array of put times from each rank
 * @param num_puts       the array of number of puts from each rank
 * @param get_times      the array of get times from each rank
 * @param num_gets       the array of number of gets from each rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetStats(hxhim_t *hx, const int dst_rank,
                    uint64_t    *put_times,
                    std::size_t *num_puts,
                    uint64_t    *get_times,
                    std::size_t *num_gets) {
    if (!hxhim::valid(hx)) {
        return HXHIM_ERROR;
    }

    const static std::size_t size_size = sizeof(std::size_t);

    // collect from all datastores first
    const std::size_t count = hx->p->datastores.size();
    uint64_t    *local_put_times = alloc_array<uint64_t>    (count);
    std::size_t *local_num_puts  = alloc_array<std::size_t> (count);
    uint64_t    *local_get_times = alloc_array<uint64_t>    (count);
    std::size_t *local_num_gets  = alloc_array<std::size_t> (count);

    auto cleanup = [local_put_times, local_num_puts,
                    local_get_times, local_num_gets,
                    count] () -> void {
        dealloc_array(local_put_times, count);
        dealloc_array(local_num_puts, count);
        dealloc_array(local_get_times, count);
        dealloc_array(local_num_gets, count);
    };

    for(std::size_t i = 0; i < count; i++) {
        if (hx->p->datastores[i]->GetStats(&local_put_times[i],
                                           &local_num_puts[i],
                                           &local_get_times[i],
                                           &local_num_gets[i]) != DATASTORE_SUCCESS) {
            cleanup();
            return HXHIM_ERROR;
        }
    }

    // send to destination rank
    MPI_Comm comm = hx->p->bootstrap.comm;
    MPI_Barrier(comm);

    if (put_times) {
        if (MPI_Gather(local_put_times, count, MPI_UINT64_T,
                             put_times, count, MPI_UINT64_T, dst_rank, comm) != MPI_SUCCESS) {
            cleanup();
            return HXHIM_ERROR;
        }
    }

    if (num_puts) {
        if (MPI_Gather(local_num_puts, size_size * count, MPI_CHAR,
                             num_puts, size_size * count, MPI_CHAR, dst_rank, comm) != MPI_SUCCESS) {
            cleanup();
            return HXHIM_ERROR;
        }
    }

    if (get_times) {
        if (MPI_Gather(local_get_times, count, MPI_UINT64_T,
                             get_times, count, MPI_UINT64_T, dst_rank, comm) != MPI_SUCCESS) {
            cleanup();
            return HXHIM_ERROR;
        }
    }

    if (num_gets) {
        if (MPI_Gather(local_num_gets, size_size * count, MPI_CHAR,
                             num_gets, size_size * count, MPI_CHAR, dst_rank, comm) != MPI_SUCCESS) {
            cleanup();
            return HXHIM_ERROR;
        }
    }

    MPI_Barrier(comm);
    cleanup();

    return HXHIM_SUCCESS;
}

/**
 * hxhimGetStats
 * Collective operation
 * Each desired pointer should be preallocated with space for md->size values
 *
 * @param hx             the HXHIM session
 * @param dst_rank       the rank that is collecting the data
 * @param put_times      the array of put times from each rank
 * @param num_puts       the array of number of puts from each rank
 * @param get_times      the array of get times from each rank
 * @param num_gets       the array of number of gets from each rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetStats(hxhim_t *hx, const int dst_rank,
                  uint64_t    *put_times,
                  std::size_t *num_puts,
                  uint64_t    *get_times,
                  std::size_t *num_gets) {
    return hxhim::GetStats(hx, dst_rank,
                           put_times,
                           num_puts,
                           get_times,
                           num_gets);
}

/**
 * Histogram
 * Get the histograms from a datastore
 *
 * @param hx         the HXHIM session
 * @param ds_id      the datastore to collect from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Histogram(hxhim_t *hx, int ds_id) {
    if (!valid(hx)  || !hx->p->running           ||
        (ds_id < 0) ||
        (ds_id >= (int) hx->p->total_datastores)) {
        return HXHIM_ERROR;
    }

    ::Stats::Chronostamp hist;
    hist.start = ::Stats::now();
    const int rc = hxhim::HistogramImpl(hx, hx->p->queues.histograms, ds_id);
    hist.end = ::Stats::now();
    hx->p->stats.single_op[hxhim_op_t::HXHIM_HISTOGRAM].emplace_back(hist);

    return rc;
}

/**
 * hxhimHistogram
 * Get the histograms from a datastore
 *
 * @param hx         the HXHIM session
 * @param ds_id      the datastore to collect from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimHistogram(hxhim_t *hx, int ds_id) {
    return hxhim::Histogram(hx, ds_id);
}

/**
 * BHistogram
 * Add multiple histogram requests into the work queue
 *
 * @param hx            the HXHIM session
 * @param ds_ids        list of datastore ids to pull histograms from
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BHistogram(hxhim_t *hx,
                      int *ds_ids,
                      const std::size_t count) {
    if (!valid(hx) || !hx->p->running) {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        if ((ds_ids[i] < 0) ||
            (ds_ids[i] >= (int) hx->p->datastores.size())) {
            return HXHIM_ERROR;
        }
    }

    ::Stats::Chronostamp bhist;
    bhist.start = ::Stats::now();
    for(std::size_t i = 0; i < count; i++) {
        hxhim::HistogramImpl(hx, hx->p->queues.histograms, ds_ids[i]);
    }

    bhist.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_HISTOGRAM].emplace_back(bhist);
    return HXHIM_SUCCESS;
}

/**
 * hxhimBHistogram
 * Add a BDEL into the work queue
 *
 * @param hx            the HXHIM session
 * @param ds_ids        list of datastore ids to pull histograms from
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBHistogram(hxhim_t *hx,
                    int *ds_ids,
                    const std::size_t count) {
    return hxhim::BHistogram(hx,
                             ds_ids,
                             count);
}

// /**
//  * GetFilled
//  * Collective operation
//  * Collects statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param rank        the rank of this instance of Transport
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @param calc        the function to use to calculate some statistic using the input data
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// static int GetFilled(MPI_Comm comm, const int rank, const int dst_rank,
//                      const bool get_bput, long double *bput,
//                      const bool get_bget, long double *bget,
//                      const bool get_bgetop, long double *bgetop,
//                      const bool get_bdel, long double *bdel,
//                      const hxhim_private_t::Stats &stats,
//                      const std::function<long double(const hxhim_private_t::Stats::Op &)> &calc) {
//     MPI_Barrier(comm);

//     if (rank == dst_rank) {
//         if (get_bput) {
//             const long double filled = calc(stats.bput);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bput, 1, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//         if (get_bget) {
//             const long double filled = calc(stats.bget);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bget, 1, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//         if (get_bgetop) {
//             const long double filled = calc(stats.bgetop);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bgetop, 1, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//         if (get_bdel) {
//             const long double filled = calc(stats.bdel);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, bdel, 1, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//     }
//     else {
//         if (get_bput) {
//             const long double filled = calc(stats.bput);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//         if (get_bget) {
//             const long double filled = calc(stats.bget);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//         if (get_bgetop) {
//             const long double filled = calc(stats.bgetop);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//         if (get_bdel) {
//             const long double filled = calc(stats.bdel);
//             MPI_Gather(&filled, 1, MPI_LONG_DOUBLE, nullptr, 0, MPI_LONG_DOUBLE, dst_rank, comm);
//         }
//     }

//     MPI_Barrier(comm);

//     return HXHIM_SUCCESS;
// }

// /**
//  * GetMinFilled
//  * Collective operation
//  * Collects statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// int hxhim::GetMinFilled(hxhim_t *hx, const int dst_rank,
//                         const bool get_bput, long double *bput,
//                         const bool get_bget, long double *bget,
//                         const bool get_bgetop, long double *bgetop,
//                         const bool get_bdel, long double *bdel) {
//     auto min_filled = [](const hxhim_private_t::Stats::Op &op) -> long double {
//         std::lock_guard<std::mutex> lock(op.mutex);
//         long double min = op.filled.size()?LDBL_MAX:0;
//         for(REF(op.filled)::value_type const &filled : op.filled) {
//             min = std::min(min, filled.percent);
//         }

//         return min;
//     };

//     return GetFilled(hx->p->bootstrap.comm, hx->p->bootstrap.rank, dst_rank,
//                      get_bput, bput,
//                      get_bget, bget,
//                      get_bgetop, bgetop,
//                      get_bdel, bdel,
//                      hx->p->stats,
//                      min_filled);
// }

// /**
//  * hxhimGetMinFilled
//  * Collective operation
//  * Collects transport statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param rank        the rank of this instance of Transport
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// int hxhimGetMinFilled(hxhim_t *hx, const int dst_rank,
//                                const int get_bput, long double *bput,
//                                const int get_bget, long double *bget,
//                                const int get_bgetop, long double *bgetop,
//                                const int get_bdel, long double *bdel) {
//     return hxhim::GetMinFilled(hx, dst_rank,
//                                get_bput, bput,
//                                get_bget, bget,
//                                get_bgetop, bgetop,
//                                get_bdel, bdel);
// }

// /**
//  * GetAverageFilled
//  * Collective operation
//  * Collects statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// int hxhim::GetAverageFilled(hxhim_t *hx, const int dst_rank,
//                             const bool get_bput, long double *bput,
//                             const bool get_bget, long double *bget,
//                             const bool get_bgetop, long double *bgetop,
//                             const bool get_bdel, long double *bdel) {
//     auto average_filled = [](const hxhim_private_t::Stats::Op &op) -> long double {
//         std::lock_guard<std::mutex> lock(op.mutex);
//         long double sum = 0;
//         for(REF(op.filled)::value_type const &filled : op.filled) {
//             sum += filled.percent;
//         }

//         return op.filled.size()?(sum / op.filled.size()):0;
//     };

//     return GetFilled(hx->p->bootstrap.comm, hx->p->bootstrap.rank, dst_rank,
//                      get_bput, bput,
//                      get_bget, bget,
//                      get_bgetop, bgetop,
//                      get_bdel, bdel,
//                      hx->p->stats,
//                      average_filled);
// }

// /**
//  * hxhimGetAverageFilled
//  * Collective operation
//  * Collects transport statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param rank        the rank of this instance of Transport
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// int hxhimGetAverageFilled(hxhim_t *hx, const int dst_rank,
//                           const int get_bput, long double *bput,
//                           const int get_bget, long double *bget,
//                           const int get_bgetop, long double *bgetop,
//                           const int get_bdel, long double *bdel) {
//     return hxhim::GetAverageFilled(hx, dst_rank,
//                                    get_bput, bput,
//                                    get_bget, bget,
//                                    get_bgetop, bgetop,
//                                    get_bdel, bdel);
// }

// /**
//  * GetMaxFilled
//  * Collective operation
//  * Collects statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// int hxhim::GetMaxFilled(hxhim_t *hx, const int dst_rank,
//                         const bool get_bput, long double *bput,
//                         const bool get_bget, long double *bget,
//                         const bool get_bgetop, long double *bgetop,
//                         const bool get_bdel, long double *bdel) {
//     auto max_filled = [](const hxhim_private_t::Stats::Op &op) -> long double {
//         std::lock_guard<std::mutex> lock(op.mutex);
//         long double max = 0;
//         for(REF(op.filled)::value_type const &filled : op.filled) {
//             max = std::max(max, filled.percent);
//         }

//         return max;
//     };

//     return GetFilled(hx->p->bootstrap.comm, hx->p->bootstrap.rank, dst_rank,
//                      get_bput, bput,
//                      get_bget, bget,
//                      get_bgetop, bgetop,
//                      get_bdel, bdel,
//                      hx->p->stats,
//                      max_filled);
// }

// /**
//  * hxhimGetMaxFilled
//  * Collective operation
//  * Collects transport statistics from all ranks in the communicator
//  *
//  * @param comm        the MPI communicator
//  * @param rank        the rank of this instance of Transport
//  * @param dst_rank    the rank to send to
//  * @param get_bput    whether or not to get bput
//  * @param bput        the array of bput from each rank
//  * @param get_bget    whether or not to get bget
//  * @param bget        the array of bget from each rank
//  * @param get_bgetop  whether or not to get bgetop
//  * @param bgetop      the array of bgetop from each rank
//  * @param get_bdel    whether or not to get bdel
//  * @param bdel        the array of bdel from each rank
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR on error
//  */
// int hxhimGetMaxFilled(hxhim_t *hx, const int dst_rank,
//                       const int get_bput, long double *bput,
//                       const int get_bget, long double *bget,
//                       const int get_bgetop, long double *bgetop,
//                       const int get_bdel, long double *bdel) {
//     return hxhim::GetMaxFilled(hx, dst_rank,
//                                get_bput, bput,
//                                get_bget, bget,
//                                get_bgetop, bgetop,
//                                get_bdel, bdel);
// }
