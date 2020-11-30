#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"

/**
 * BHistogram
 * Add multiple histogram requests into the work queue
 *
 * @param hx            the HXHIM session
 * @param rs_ids        list of datastore ids to pull histograms from
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BHistogram(hxhim_t *hx,
                      int *rs_ids,
                      const char **names, const std::size_t *name_lens,
                      const std::size_t count) {
    if (!valid(hx) || !hx->p->running ||
        !rs_ids    ||
        !names     || !name_lens)      {
        return HXHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        if ((rs_ids[i] < 0) ||
            (rs_ids[i] >= (int) hx->p->range_server.total_range_servers)) {
            return HXHIM_ERROR;
        }

        // // make sure the name matches a histogram
        // REF(hx->p->hist_names)::const_iterator name_it = hx->p->hist_names.find(std::string(name, name_len));
        // if (name_it == hx->p->hist_names.end()) {
        //     return HXHIM_ERROR;
        // }
    }

    ::Stats::Chronostamp bhist;
    bhist.start = ::Stats::now();
    for(std::size_t i = 0; i < count; i++) {
        hxhim::HistogramImpl(hx, hx->p->queues.histograms, rs_ids[i], names[i], name_lens[i]);
    }

    bhist.end = ::Stats::now();
    hx->p->stats.bulk_op[hxhim_op_t::HXHIM_HISTOGRAM].emplace_back(bhist);
    return HXHIM_SUCCESS;
}

/**
 * hxhimBHistogram
 * Add a BHISTOGRAM into the work queue
 *
 * @param hx            the HXHIM session
 * @param rs_ids        list of datastore ids to pull histograms from
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBHistogram(hxhim_t *hx,
                    int *rs_ids,
                    const char **names, const size_t *name_lens,
                    const size_t count) {
    return hxhim::BHistogram(hx,
                             rs_ids,
                             names, name_lens,
                             count);
}
