#include <cmath>
#include <limits>
#include <stdexcept>

#include "utils/Histogram.hpp"

static int minmax(const std::list<double> &values, double &min, double &max) {
    // get min and max
    min = std::numeric_limits<double>::max();
    max = std::numeric_limits<double>::min();
    for(double const &value: values) {
        if (value < min) {
            min = value;
        }

        if (value > max) {
            max = value;
        }
    }

    return Histogram::SUCCESS;
}

static int generate_using_bin_count(const double &min, const double &max, const std::size_t bins, std::map<double, std::size_t> &histogram) {
    histogram.clear();

    // generate the left ends of the buckets
    const double step = ((double) (max - min)) / bins;
    double bin = min;
    for(std::size_t i = 0; i < bins; i++) {
        histogram[bin] = 0;
        bin += step;
    }
    return Histogram::SUCCESS;
}

namespace Histogram {

int BucketGen::n_buckets(const std::list<double> &values, std::map<double, std::size_t> &histogram, void *extra) {
    if (!values.size() | !extra) {
        return ERROR;
    }

    double min, max;
    minmax(values, min, max);

    histogram.clear();

    const std::size_t buckets = * (std::size_t *) extra;
    const double width = std::ceil((max - min) / buckets);
    for(std::size_t i = 0; i < buckets; i++) {
        histogram[min] = 0;
        min += width;
    }

    return SUCCESS;
}

int BucketGen::square_root_choice(const std::list<double> &values, std::map<double, std::size_t> &histogram, void *) {
    if (!values.size()) {
        return ERROR;
    }

    double min, max;
    minmax(values, min, max);

    return generate_using_bin_count(min, max, std::sqrt(values.size()), histogram);
}

int BucketGen::sturges_formula(const std::list<double> &values, std::map<double, std::size_t> &histogram, void *) {
    if (!values.size()) {
        return ERROR;
    }

    double min, max;
    minmax(values, min, max);

    return generate_using_bin_count(min, max, std::log2(values.size()) + 2, histogram);
}

int BucketGen::rice_rule(const std::list<double> &values, std::map<double, std::size_t> &histogram, void *) {
    if (!values.size()) {
        return ERROR;
    }

    double min, max;
    minmax(values, min, max);

    return generate_using_bin_count(min, max, std::cbrt(values.size()) + 2, histogram);
}

int BucketGen::scotts_normal_reference_rule(const std::list<double> &values, std::map<double, std::size_t> &histogram, void *) {
    if (!values.size()) {
        return ERROR;
    }

    double min, max;
    minmax(values, min, max);

    // calculate the standard deviation
    double mean = 0;
    for(double const &value: values) {
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

int BucketGen::uniform_logn(const std::list<double> &values, std::map<double, std::size_t> &histogram, void *extra) {
    if (!values.size() || !extra) {
        return ERROR;
    }

    double min, max;
    minmax(values, min, max);

    const std::size_t n = * (std::size_t *) extra;

    return generate_using_bin_count(min, max, std::ceil((max - min) * std::log(n) / std::log(max - min)), histogram);
}


Histogram::Histogram(const std::size_t use_first_n, const BucketGen::generator &generator, void *extra_args)
    : first_n(use_first_n),
      gen(generator),
      extra(extra_args),
      data(),
      hist(),
      count(0)
{
    if (!generator) {
        throw std::runtime_error("Bad bucket generator function");
    }
}

Histogram::~Histogram() {}

/**
 * add
 * If there are not enough values (< limit), adds a value
 * to the list of values used to generate the bins.
 * If the new value makes the list large enough, the
 * bins are generated, and the values in the list are added.
 * If (including the new value), there are more values
 * than the limit, adds a value to the histogram.
 *
 * @param value a pointer to a value of type T, typecasted to a void *
 * @return SUCCESS or ERROR
 */
int Histogram::add(const double &value) {
    // limit has not been hit
    if (data.size() < first_n) {
        data.push_back(value);

        // if the limit has been hit with this new value
        // generate and fill bins
        if (data.size() == first_n) {
            gen(data, hist, extra);

            // insert original data
            for(double const &val : data) {
                insert(val);
            }
        }
    }
    // do not insert into the histogram until the limit has been hit
    else {
        insert(value);
    }

    count++;

    return SUCCESS;
}

const std::map<double, std::size_t> &Histogram::get() const {
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
int Histogram::insert(const double &value) {
    if (!hist.size()) {
        return ERROR;
    }

    std::map<double, std::size_t>::iterator it = hist.upper_bound(value);
    if (it != hist.begin()) {
        --it;
    }
    it->second++;

    return SUCCESS;
}

}
