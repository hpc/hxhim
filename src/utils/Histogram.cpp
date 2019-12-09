#include <algorithm>
#include <cmath>
#include <iterator>
#include <limits>
#include <stdexcept>

#include "utils/Histogram.hpp"

static int minmax(const double *values, const std::size_t len, double &min, double &max) {
    const double *min_it = std::min_element(values, values + len);

    if (min_it == (values + len)) {
        min = std::numeric_limits<double>::max();
    }
    else {
        min = *min_it;
    }

    const double *max_it = std::max_element(values, values + len);

    if (max_it == (values + len)) {
        max = std::numeric_limits<double>::min();
    }
    else {
        max = *max_it;
    }

    return HISTOGRAM_SUCCESS;
}

static int generate_using_bin_count(const double &min, const double &max, const std::size_t bin_count, double **buckets, size_t *size) {
    if (!buckets || !size) {
        return HISTOGRAM_ERROR;
    }

    double *bins = alloc_array<double>(bin_count);
    if (!bins) {
        return HISTOGRAM_ERROR;
    }

    // generate the left ends of the buckets
    const double step = ((double) (max - min)) / bin_count;
    double left = min;
    for(std::size_t i = 0; i < bin_count; i++) {
        bins[i] = left;
        left += step;
    }

    *size = bin_count;
    *buckets = bins;

    return HISTOGRAM_SUCCESS;
}

int histogram_n_buckets(const double *first_n, const size_t n, double **buckets, size_t *size, void *extra) {
    if (!first_n || !n              ||
        !buckets || !size || !extra) {
        return HISTOGRAM_ERROR;
    }

    *size = * (std::size_t *) extra;
    if (!(*buckets = alloc_array<double>(*size))) {
        return HISTOGRAM_ERROR;
    }

    double min, max;
    minmax(first_n, n, min, max);

    const double width = std::ceil((max - min) / *size);
    for(std::size_t i = 0; i < *size; i++) {
        (*buckets)[i] = min;
        min += width;
    }

    return HISTOGRAM_SUCCESS;
}

int histogram_square_root_choice(const double *first_n, const size_t n, double **buckets, size_t *size, void *) {
    if (!first_n || !n    ||
        !buckets || !size) {
        return HISTOGRAM_ERROR;
    }

    double min, max;
    minmax(first_n, n, min, max);

    return generate_using_bin_count(min, max, std::sqrt(n), buckets, size);
}

int histogram_sturges_formula(const double *first_n, const size_t n, double **buckets, size_t *size, void *) {
    if (!first_n || !n    ||
        !buckets || !size) {
        return HISTOGRAM_ERROR;
    }

    double min, max;
    minmax(first_n, n, min, max);

    return generate_using_bin_count(min, max, std::log2(n) + 2, buckets, size);
}

int histogram_rice_rule(const double *first_n, const size_t n, double **buckets, size_t *size, void *) {
    if (!first_n || !n    ||
        !buckets || !size) {
        return HISTOGRAM_ERROR;
    }

    double min, max;
    minmax(first_n, n, min, max);

    return generate_using_bin_count(min, max, std::cbrt(n) + 2, buckets, size);
}

int histogram_scotts_normal_reference_rule(const double *first_n, const size_t n, double **buckets, size_t *size, void *) {
    if (!first_n || !n    ||
        !buckets || !size) {
        return HISTOGRAM_ERROR;
    }

    double min, max;
    minmax(first_n, n, min, max);

    // calculate the standard deviation
    double mean = 0;
    for(std::size_t i = 0; i < n; i++) {
        mean += first_n[i];
    }
    mean /= n;

    double sumsqr = 0;
    for(std::size_t i = 0; i < n; i++) {
        const double diff = (first_n[i] - mean);
        sumsqr += diff * diff;
    }

    const double stdev = std::sqrt(sumsqr / (n - 1));

    // width of a bin
    const double h = 3.5 * stdev / std::cbrt(n);

    return generate_using_bin_count(min, max, (min - max) / h, buckets, size);
}

int histogram_uniform_logn(const double *first_n, const size_t n, double **buckets, size_t *size, void *extra) {
    if (!first_n || !n              ||
        !buckets || !size || !extra) {
        return HISTOGRAM_ERROR;
    }

    double min, max;
    minmax(first_n, n, min, max);

    return generate_using_bin_count(min, max, std::ceil((max - min) * std::log(* (std::size_t *) extra) / std::log(max - min)), buckets, size);
}

namespace Histogram {

Histogram::Histogram(const std::size_t use_first_n, const HistogramBucketGenerator_t &generator, void *extra_args)
    : first_n(use_first_n),
      gen(generator),
      extra(extra_args),
      data(nullptr),
      data_size(0),
      buckets_(nullptr),
      counts_(nullptr),
      size_(0)
{
    if (!gen) {
        throw std::runtime_error("Bad bucket generator function");
    }

    if (!(data = new double[first_n]())) {
        throw std::runtime_error("Could not allocate space for first n values");
    }
}

Histogram::~Histogram() {
    clear();
    delete [] data;
}

/**
 * add
 * If there are not enough values (< limit), adds a value
 * to the list of values used to generate the bins.
 * If the new value makes the list large enough, the
 * bins are generated, and the values in the list are added.
 * If (including the new value), there are more values
 * than the limit, adds a value to the histogram.
 *
 * @param value the value to insert
 * @return HISTOGRAM_SUCCESS or HISTOGRAM_ERROR
 */
int Histogram::add(const double &value) {
    // limit has not been hit
    if (data_size < first_n) {
        data[data_size] = value;
    }

    data_size++;

    // insert into the buckets if the limit has been hit
    if (data_size >= first_n) {
        if (!buckets_) {
            // generate the buckets and allocate space for the counts
            if ((gen(data, data_size, &buckets_, &size_, extra) != HISTOGRAM_SUCCESS) ||
                (gen_counts()                                   != HISTOGRAM_SUCCESS)) {
                return HISTOGRAM_ERROR;
            }

            // insert original data
            for(std::size_t i = 0; i < first_n; i++) {
                insert(data[i]);
            }
        }
        else {
            insert(value);
        }
    }

    return HISTOGRAM_SUCCESS;
}

/**
 * get
 * Accessor for the histogram data.
 * The resulting pointers will be nullptr
 * if the number of values required to generate
 * the buckets has not been reached.
 *
 * @param buckets pointer to the buckets (optional)
 * @param counts  pointer to the counts  (optional)
 * @param size    pointer to the size    (optional)
 * @return HISTOGRAM_SUCCESS
 */
int Histogram::get(double **buckets, std::size_t **counts, std::size_t *size) const {
    if (buckets) {
        *buckets = buckets_;
    }

    if (counts) {
        *counts = counts_;
    }

    if (size) {
        *size = size_;
    }

    return HISTOGRAM_SUCCESS;
}

/**
 * clear
 * Clears all existing data, including
 * the first n values.
 */
void Histogram::clear() {
    data_size = 0;

    dealloc_array(buckets_, size_);
    buckets_ = nullptr;

    dealloc_array(counts_, size_);
    counts_ = nullptr;

    size_ = 0;
}

/**
 * gen_counts
 * Allocates memory for the counts and zeros them
 *
 * @return HISTOGRAM_SUCCESS, or HISTOGRAM_ERROR on error
 */
int Histogram::gen_counts() {
    if (data_size < first_n) {
        return HISTOGRAM_ERROR;
    }

    if (size_) {
        counts_ = alloc_array<std::size_t>(size_);
    }

    return counts_?HISTOGRAM_SUCCESS:HISTOGRAM_ERROR;
}

/**
 * insert
 * Inserts a value into the currently existing histogram.
 * Should not be called unless the histogram is ready to be
 * inserted into.
 *
 * @param value the value whose bin should be incremented
 */
int Histogram::insert(const double &value) {
    if (!buckets_ || !counts_ || !size_) {
        return HISTOGRAM_ERROR;
    }

    const std::size_t i = std::upper_bound(buckets_, buckets_ + size_, value) - buckets_ - 1;
    if (i == (std::size_t) -1) {
        return HISTOGRAM_ERROR;
    }

    counts_[i]++;

    return HISTOGRAM_SUCCESS;
}

}
