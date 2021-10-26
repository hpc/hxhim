#include "hxhim/private/Results.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "hxhim/private/process.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

hxhim_private::hxhim_private()
    : epoch(::Stats::init()),
      bootstrap(),
      running(false),
      queues(),
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
void hxhim::wait_for_background_puts(hxhim_t *hx, const bool clear_queued) {
    // lock async_put.mutex to prevent background thread from completing a loop
    // or to wait until the previous loop has completed
    std::unique_lock<std::mutex> lock(hx->p->async_put.mutex);
    hx->p->async_put.done_check = false;

    // force the background thread to flush
    {
        std::lock_guard <std::mutex> flush_lock(hx->p->queues.puts.mutex);
        if (clear_queued) {
            // clear each target range server's queue
            for(hxhim::QueueTarget<Message::Request::BPut> &target : hx->p->queues.puts.queue) {
                // clear out queued items for a target single range server
                for(Message::Request::BPut *bput : target) {
                    destruct(bput);
                }
                target.clear();
            }
            hx->p->queues.puts.queue.clear();
        }
        hx->p->queues.puts.flushed = true;
    }

    hx->p->queues.puts.start_processing.notify_all();

    // wait for the background thread to finish
    hx->p->async_put.done.wait(lock,
                               [hx]() -> bool {
                                   return !hx->p->running || hx->p->async_put.done_check;
                               });
}

#else

/**
 * serial_puts
 * This is effectively FlushPuts but runs in hxhim::Put and hxhim::BPut
 * to simulate background PUTs when threading is not allowed. The results
 * are placed into the background PUTs results buffer.
 *
 * There is no need to lock here since it is serial.
 *
 * @param hx the HXHIM session
 */
void hxhim::serial_puts(hxhim_t *hx) {
    if (hx->p->queues.puts.count >= hx->p->async_put.max_queued) {
        // don't call FlushPuts to avoid deallocating old Results only to allocate a new one
        hxhim::Results *res = hxhim::process<Message::Request::BPut, Message::Response::BPut>(hx, hx->p->queues.puts.queue);

        {
            // hold results in async_puts
            if (hx->p->async_put.results) {
                hx->p->async_put.results->Append(res);
                hxhim::Results::Destroy(res);
            }
            else {
                hx->p->async_put.results = res;
            }

            hx->p->queues.puts.count = 0;
        }
    }
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
                              hx->p->queues.max_per_request.ops,
                              hx->p->epoch,
                              stream, indent);
}
