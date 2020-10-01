#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/process.hpp"

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
                          hxhim::Unsent<UserData_t> &unsent) {
    return hxhim::process<UserData_t, Request_t, Response_t>(hx, unsent.take());
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
    hxhim::Results *put_results = FlushImpl<hxhim::PutData, Transport::Request::BPut, Transport::Response::BPut>(hx, hx->p->queues.puts);
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
    hxhim::Results *res = FlushImpl<hxhim::GetData, Transport::Request::BGet, Transport::Response::BGet>(hx, hx->p->queues.gets);
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
    hxhim::Results *res = FlushImpl<hxhim::GetOpData, Transport::Request::BGetOp, Transport::Response::BGetOp>(hx, hx->p->queues.getops);
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
    hxhim::Results *res = FlushImpl<hxhim::DeleteData, Transport::Request::BDelete, Transport::Response::BDelete>(hx, hx->p->queues.deletes);
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
    hxhim::Results *res = FlushImpl<hxhim::HistogramData, Transport::Request::BHistogram, Transport::Response::BHistogram>(hx, hx->p->queues.histograms);
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
