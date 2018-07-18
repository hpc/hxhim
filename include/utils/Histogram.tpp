#include <cmath>
#include <limits>
#include <stdexcept>

template <typename T, typename Convert2FP>
Histogram<T, Convert2FP>::Histogram(const std::size_t use_first_n, const typename HistogramBucketGen<T,Convert2FP>::generator &generator, void *extra_args)
    : HistogramBase(),
      first_n(use_first_n),
      gen(generator),
      extra(extra_args),
      data(),
      hist()
{
    if (!generator) {
        throw std::runtime_error("Bad bucket generator function");
    }

    count = 0;
}

/**
 * add
 * If there are not enouch values (< limit), adds a value
 * to the list of values used to generate the bins.
 * If the new value makes the list large enough, the
 * bins are generated, and the values in the list are added.
 * If (including the new value), there are more values
 * than the limit, adds a value to the histogram.
 *
 * @param value a pointer to a value of type T, typecasted to a void *
 * @return HISTOGRAM_SUCCESS or HISTOGRAM_ERROR
 */
template <typename T, typename Convert2FP>
int Histogram<T, Convert2FP>::add(const void *value) {
    if (!value) {
        return HISTOGRAM_ERROR;
    }

    count++;

    // limit has not been hit
    if (data.size() < first_n) {
        data.push_back(* (T *) value);

        // if the limit has been hit with this new value
        // generate and fill bins
        if (data.size() == first_n) {
            gen(data, hist, extra);

            // insert original data
            for(T const &val : data) {
                insert(val);
            }
        }
    }
    // do not insert into the histogram until the limit has been hit
    else {
        insert(* (T *) value);
    }

    return HISTOGRAM_SUCCESS;
}

/**
 * add
 * Add a value of type T into the histogram
 *
 * @param value the value to add
 * @return HISTOGRAM_SUCCESS or HISTOGRAM_ERROR
 */
template <typename T, typename Convert2FP>
int Histogram<T, Convert2FP>::add(const T &value) {
    return add(&value);
}

template <typename T, typename Convert2FP>
const std::map<double, std::size_t> &Histogram<T, Convert2FP>::get() const {
    return hist;
}

/**
 * insert
 * Inserts a value into the currently existing histogram.
 * Should not be called unless the histogram is ready to be
 * inserted into.
 *
 * @param value the value whose bin should be incremented
 */
template <typename T, typename Convert2FP>
int Histogram<T, Convert2FP>::insert(const T &value) {
    if (!hist.size()) {
        return HISTOGRAM_ERROR;
    }

    std::map<double, std::size_t>::iterator it = hist.upper_bound(value);
    if (it != hist.begin()) {
        --it;
    }
    it->second++;

    return HISTOGRAM_SUCCESS;
}

template <typename T>
int minmax(const std::list<T> &values, T &min, T &max) {
    // get min and max
    min = std::numeric_limits<T>::max();
    max = std::numeric_limits<T>::min();
    for(T const &value: values) {
        if (value < min) {
            min = value;
        }

        if (value > max) {
            max = value;
        }
    }

    return HISTOGRAM_SUCCESS;
}

template <typename T>
int generate_using_bin_count(const T &min, const T &max, const std::size_t bins, std::map<double, std::size_t> &histogram) {
    histogram.clear();

    // generate the left ends of the buckets
    const double step = ((double) (max - min)) / bins;
    double bin = min;
    for(std::size_t i = 0; i < bins; i++) {
        histogram[bin] = 0;
        bin += step;
    }
    return HISTOGRAM_SUCCESS;
}

template <typename T, typename Convert2FP>
int HistogramBucketGen<T,Convert2FP>::n_buckets(const std::list<T> &values, std::map<double, std::size_t> &histogram, void *extra) {
    if (!values.size() | !extra) {
        return HISTOGRAM_ERROR;
    }

    T min, max;
    minmax(values, min, max);

    histogram.clear();

    const std::size_t buckets = * (std::size_t *) extra;

    const double width = std::ceil((max - min) / buckets);
    for(std::size_t i = 0; i < buckets; i++) {
        histogram[min] = 0;
        min += width;
    }

    return HISTOGRAM_SUCCESS;
}

template <typename T, typename Convert2FP>
int HistogramBucketGen<T,Convert2FP>::square_root_choice(const std::list<T> &values, std::map<double, std::size_t> &histogram, void *) {
    if (!values.size()) {
        return HISTOGRAM_ERROR;
    }

    T min, max;
    minmax(values, min, max);

    return generate_using_bin_count(min, max, std::sqrt(values.size()), histogram);
}

template <typename T, typename Convert2FP>
int HistogramBucketGen<T,Convert2FP>::sturges_formula(const std::list<T> &values, std::map<double, std::size_t> &histogram, void *) {
    if (!values.size()) {
        return HISTOGRAM_ERROR;
    }

    T min, max;
    minmax(values, min, max);

    return generate_using_bin_count(min, max, std::log2(values.size()) + 2, histogram);
}

template <typename T, typename Convert2FP>
int HistogramBucketGen<T,Convert2FP>::rice_rule(const std::list<T> &values, std::map<double, std::size_t> &histogram, void *) {
    if (!values.size()) {
        return HISTOGRAM_ERROR;
    }

    T min, max;
    minmax(values, min, max);

    return generate_using_bin_count(min, max, std::cbrt(values.size()) + 2, histogram);
}

template <typename T, typename Convert2FP>
int HistogramBucketGen<T,Convert2FP>::scotts_normal_reference_rule(const std::list<T> &values, std::map<double, std::size_t> &histogram, void *) {
    if (!values.size()) {
        return HISTOGRAM_ERROR;
    }

    T min, max;
    minmax(values, min, max);

    // calculate the standard deviation
    double mean = 0;
    for(T const &value: values) {
        mean += value;
    }
    mean /= values.size();

    double sumsqr = 0;
    for(double const & value: values) {
        const double diff = (value - mean);
        sumsqr += diff * diff;
    }

    const double stdev = std::sqrt(sumsqr / (values.size() - 1));

    // width of a bin
    const double h = 3.5 * stdev / std::cbrt(values.size());

    return generate_using_bin_count(min, max, (min - max) / h, histogram);
}

template <typename T, typename Convert2FP>
int HistogramBucketGen<T,Convert2FP>::uniform_logn(const std::list<T> &values, std::map<double, std::size_t> &histogram, void *extra) {
    if (!values.size() || !extra) {
        return HISTOGRAM_ERROR;
    }

    T min, max;
    minmax(values, min, max);

    const std::size_t n = * (std::size_t *) extra;

    return generate_using_bin_count(min, max, std::ceil((max - min) * std::log(n) / std::log(max - min)), histogram);
}
