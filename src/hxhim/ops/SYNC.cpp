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
