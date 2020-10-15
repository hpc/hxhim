#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"

/**
 * Histogram
 * Get the histograms from a datastore
 *
 * @param hx         the HXHIM session
 * @param ds_id      the datastore to collect from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Histogram(hxhim_t *hx, int ds_id) {
    if (!valid(hx)  || !hx->p->running                           ||
        (ds_id < 0) ||
        (ds_id >= (int) hx->p->range_server.total_range_servers)) {
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
