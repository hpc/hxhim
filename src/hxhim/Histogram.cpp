#include "hxhim/Histogram.hpp"
#include "hxhim/Histogram_private.hpp"
#include "hxhim/accessors_private.hpp"
#include "utils/memory.hpp"

int hxhim::histogram::init(hxhim_t *hx, hxhim_histogram_t **hists) {
    if (!hists) {
        return HXHIM_ERROR;
    }

    std::size_t total_ds = 0;
    hxhim::nocheck::GetDatastoreCount(hx, &total_ds);

    *hists = construct<hxhim_histogram_t>();
    (*hists)->p = construct<hxhim_histogram_private_t>();
    (*hists)->p->hists.resize(total_ds);

    return HXHIM_SUCCESS;
}

/**
 * valid
 * Quick check to see if histogram struct contains data
 *
 * @param hists the public histogram struct
 * @param true or false
 */
static bool valid(hxhim_histogram_t *hists) {
    return hists && hists->p;
}

/**
 * count
 * Get the number of histograms there are
 *
 * @param hists the public histogram struct
 * @param count the count
 * @param HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::histogram::count(hxhim_histogram_t *hists, std::size_t *count) {
    if (!valid(hists)) {
        return HXHIM_ERROR;
    }

    if (count) {
        *count = hists->p->hists.size();
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_histogram_count
 * Get the number of histograms there are
 *
 * @param hists the public histogram struct
 * @param count the count
 * @param HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_histogram_count(hxhim_histogram_t *hists, size_t *count) {
    return hxhim::histogram::count(hists, count);
}

/**
 * get
 * Extract the histogram data from the given index
 *
 * @param hists   the public histogram struct
 * @param idx     the index of the histogram
 * @param buckets address of the buckets array
 * @param counts  address of the counts array
 * @param count   how many bucket/count pairs there are
 * @param HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::histogram::get(hxhim_histogram_t *hists, const std::size_t idx,
                          double **buckets, std::size_t **counts, std::size_t *count) {
    if (!valid(hists)) {
        return HXHIM_ERROR;
    }

    if (idx >= hists->p->hists.size()) {
        return HXHIM_ERROR;
    }

    // shouldn't happen, but check just in case
    if (!hists->p->hists[idx]) {
        return HXHIM_ERROR;
    }

    if (hists->p->hists[idx]->get(buckets, counts, count) != HISTOGRAM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_histogram_get
 * Extract the histogram data from the given index
 *
 * @param hists   the public histogram struct
 * @param idx     the index of the histogram
 * @param buckets address of the buckets array
 * @param counts  address of the counts array
 * @param count   how many bucket/count pairs there are
 * @param HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_histogram_get(hxhim_histogram_t *hists, const size_t idx,
                        double **buckets, size_t **counts, size_t *count) {
    return hxhim::histogram::get(hists, idx, buckets, counts, count);
}

/**
 * destroy
 * clean up histograms through the public interface
 *
 * @param hists the public histogram struct
 * @param HXHIM_SUCCESS
 */
int hxhim::histogram::destroy(hxhim_histogram_t *hists) {
    if (hists) {
        if (hists->p) {
            for(Histogram::Histogram *hist : hists->p->hists) {
                destruct(hist);
            }
        }
        destruct(hists->p);
        hists->p = nullptr;
    }
    destruct(hists);

    return HXHIM_SUCCESS;
}

/**
 */
int hxhim_histogram_destroy(hxhim_histogram_t *hists) {
    return hxhim::histogram::destroy(hists);
}
