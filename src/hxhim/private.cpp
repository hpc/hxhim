#include "hxhim/private/Results.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "hxhim/private/process.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

hxhim_private::hxhim_private()
    : epoch(::Stats::init()),
      bootstrap(),
      running(false),
      max_ops_per_send(),
      queues(),
      datastores(),
      async_put(),
      hash(),
      transport(nullptr),
      range_server(),
      stats(),
      print_buffer()
{}

hxhim_private::~hxhim_private() {
    mlog(HXHIM_CLIENT_NOTE, "\n%s", print_buffer.str().c_str());
}

#if ASYNC_PUTS
/**
 * backgroundPUT
 * The thread that runs when the number of full batches crosses the queued bputs threshold
 *
 * @param hx      the HXHIM context
 */
static void backgroundPUT(hxhim_t *hx) {
    if (!hxhim::valid(hx)) {
        return;
    }

    mlog(HXHIM_CLIENT_DBG, "Started background PUT thread");

    while (hx->p->running) {
        hxhim::PutData *head = nullptr;    // the first PUT to process

        hxhim::Unsent<hxhim::PutData> &unsent = hx->p->queues.puts;

        // hold unsent.mutex just long enough to move queued PUTs to send queue
        {
            // Wait until any of the following is true
            //    1. HXHIM is no longer running
            //    2. The number of queued PUTs passes the threshold
            //    3. The PUTs are being forced to flush
            std::unique_lock<std::mutex> lock(unsent.mutex);
                mlog(HXHIM_CLIENT_DBG, "Waiting for %zu PUTs (currently have %zu)", hx->p->async_put.max_queued, unsent.count);
                unsent.start_processing.wait(lock, [&]() -> bool { return !hx->p->running || (unsent.count >= hx->p->async_put.max_queued); });

            mlog(HXHIM_CLIENT_DBG, "Moving %zu queued PUTs into process queue", unsent.count);

            // move all PUTs into this thread for processing
            head = unsent.take_no_lock();
        }

        mlog(HXHIM_CLIENT_DBG, "Processing queued PUTs");
        {
            // process the queued PUTs
            hxhim::Results *res = hxhim::process<hxhim::PutData, Transport::Request::BPut, Transport::Response::BPut>(hx, head, hx->p->max_ops_per_send);

            // store the results in a buffer that FlushPuts will clean up
            {
                std::lock_guard<std::mutex> lock(hx->p->async_put.mutex);

                if (hx->p->async_put.results) {
                    hx->p->async_put.results->Append(res);
                    destruct(res);
                }
                else {
                    hx->p->async_put.results = res;
                }
            }

            unsent.done_processing.notify_all();
        }

        mlog(HXHIM_CLIENT_DBG, "Done processing queued PUTs");
    }

    mlog(HXHIM_CLIENT_DBG, "Background PUT thread stopping");
}
#else
/**
 * serial_puts
 * This is effectively FlushPuts but runs in hxhim::Put and hxhim::BPut
 * to simulate background PUTs when threading is not allowed. The results
 * are placed into the background PUTs results buffer.
 *
 * @param hx the HXHIM session
 */
void hxhim::serial_puts(hxhim_t *hx) {
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
 * valid
 * Checks if hx is valid
 *
 * @param hx   the HXHIM instance
 * @param true if ready, else false
 */
bool hxhim::valid(hxhim_t *hx) {
    return hx && hx->p;
}

/**
 * valid
 * Checks if opts are valid
 *
 * @param opts the HXHIM options
 * @param true if ready, else false
 */
bool hxhim::valid(hxhim_options_t *opts) {
    return opts && opts->p;
}

/**
 * valid
 * Checks if hx and opts are ready to be used
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @param true if ready, else false
 */
bool hxhim::valid(hxhim_t *hx, hxhim_options_t *opts) {
    return valid(hx) && valid(opts);
}


std::ostream &hxhim::print_stats(hxhim_t *hx,
                                 std::ostream &stream,
                                 const std::string &indent) {
    return hx->p->stats.print(hx->p->bootstrap.rank,
                              hx->p->max_ops_per_send,
                              hx->p->epoch,
                              stream, indent);
}
