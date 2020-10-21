#include "hxhim/hxhim.hpp"
#include "hxhim/private/Results.hpp"
#include "hxhim/private/hxhim.hpp"

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

    // clear out queues
    hxhim::Results *res = Flush(hx);

    MPI_Barrier(hx->p->bootstrap.comm);

    ::Stats::Send send;
    send.hash.start        = ::Stats::now();
    send.hash.end          = ::Stats::now();
    send.insert.start      = ::Stats::now();
    send.insert.end        = ::Stats::now();

    struct ::Stats::SendRecv transport;

    transport.pack.start   = ::Stats::now();
    transport.pack.end     = ::Stats::now();

    transport.send_start   = ::Stats::now();
    transport.recv_end     = ::Stats::now();

    transport.unpack.start = ::Stats::now();
    transport.unpack.end   = ::Stats::now();

    // Sync local datastore
    transport.start        = ::Stats::now();

    // clear out local datastore
    // if there is no local datastore, treat as success
    const int synced = hx->p->datastore?hx->p->datastore->Sync():DATASTORE_SUCCESS;
    hxhim::Results::Sync *sync = hxhim::Result::init(hx, synced);

    sync->timestamps.send = std::move(send);
    sync->timestamps.transport = transport;
    sync->timestamps.transport.end = ::Stats::now();
    sync->timestamps.recv.result.start = ::Stats::now();
    res->Add(sync);
    sync->timestamps.recv.result.end = ::Stats::now();

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
