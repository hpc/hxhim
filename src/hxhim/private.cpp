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
      datastore(nullptr),
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

#if !ASYNC_PUTS
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
