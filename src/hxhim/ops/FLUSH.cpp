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
template <typename Request_t, typename Response_t>
hxhim::Results *FlushImpl(hxhim_t *hx,
                          hxhim::Queues<Request_t> &unsent) {
    return hxhim::process<Request_t, Response_t>(hx, unsent);
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
    const int rank = hx->p->bootstrap.rank;

    mlog(HXHIM_CLIENT_INFO, "Rank %d Flushing PUTs", rank);

    hxhim::Results *res = construct<hxhim::Results>();

    if (hx->p->async_puts.enabled) {
        // wait for the background thread to finish
        hxhim::wait_for_background_puts(hx, false);

        // move internal results to res to give to caller
        res->Append(hx->p->async_puts.results);
        destruct(hx->p->async_puts.results);
        hx->p->async_puts.results = nullptr;

        hx->p->queues.puts.mutex.lock();
    }

    // append new results to old results
    hxhim::Results *put_results =
        FlushImpl<Message::Request::BPut,
                  Message::Response::BPut>(hx, hx->p->queues.puts.queue);
    hx->p->queues.puts.count = 0;

    res->Append(put_results);
    destruct(put_results);

    if (hx->p->async_puts.enabled) {
        hx->p->queues.puts.mutex.unlock();
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
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Flushing GETs", rank);
    hxhim::Results *res = FlushImpl<Message::Request::BGet, Message::Response::BGet>(hx, hx->p->queues.gets);
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
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Flushing GETOPs", rank);
    hxhim::Results *res = FlushImpl<Message::Request::BGetOp, Message::Response::BGetOp>(hx, hx->p->queues.getops);
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
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Flushing DELETEs", rank);
    hxhim::Results *res = FlushImpl<Message::Request::BDelete, Message::Response::BDelete>(hx, hx->p->queues.deletes);
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
    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(HXHIM_CLIENT_INFO, "Rank %d Flushing HISTOGRAMs", rank);
    hxhim::Results *res = FlushImpl<Message::Request::BHistogram, Message::Response::BHistogram>(hx, hx->p->queues.histograms);
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
    hxhim::Results *res    = construct<hxhim::Results>();

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
