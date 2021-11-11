#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"

/**
 * Histogram
 * Get the histograms from a datastore
 *
 * @param hx         the HXHIM session
 * @param rs_id      the datastore to collect from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Histogram(hxhim_t *hx, int rs_id,
                     const char *name, const std::size_t len) {
    if (!started(hx) ||
        (rs_id < 0)  ||
        (rs_id >= (int) hx->p->range_server.total_range_servers)) {
        return HXHIM_ERROR;
    }

    // // make sure the name matches a histogram
    // REF(hx->p->hist_names)::const_iterator name_it = hx->p->hist_names.find(std::string(name, name_len));
    // if (name_it == hx->p->hist_names.end()) {
    //     return HXHIM_ERROR;
    // }

    ::Stats::Chronostamp hist;
    hist.start = ::Stats::now();
    const int rc = hxhim::HistogramImpl(hx, hx->p->queues.histograms, rs_id, name, len);
    hist.end = ::Stats::now();
    hx->p->stats.single_op[hxhim_op_t::HXHIM_HISTOGRAM].emplace_back(hist);

    return rc;
}

/**
 * hxhimHistogram
 * Get the histograms from a datastore
 *
 * @param hx         the HXHIM session
 * @param rs_id      the datastore to collect from
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimHistogram(hxhim_t *hx, int rs_id,
                   const char *name, const size_t len) {
    return hxhim::Histogram(hx, rs_id, name, len);
}
