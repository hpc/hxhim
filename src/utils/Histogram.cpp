#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <sstream>
#include <stdexcept>

#include "utils/Histogram.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

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

    double *bins = alloc_array<double>(bin_count + 1);

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

    *size = (std::size_t) (uintptr_t) extra;
    *buckets = alloc_array<double>(*size + 1);

    double min = 0, max = 0;
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

    return generate_using_bin_count(min, max, std::ceil((max - min) * std::log((std::size_t) (uintptr_t) extra) / std::log(max - min)), buckets, size);
}

Histogram::Histogram::Histogram()
    : name_(),
      first_n_(0),
      gen_(nullptr),
      extra_(nullptr),
      cache_(nullptr),
      count_(0),
      buckets_(nullptr),
      counts_(nullptr),
      size_(0)
{}

Histogram::Histogram::Histogram(const Config &config, const std::string &name)
    : name_(name),
      first_n_(config.first_n),
      gen_(config.generator),
      extra_(config.extra_args),
      cache_(alloc_array<double>(first_n_)),
      count_(0),
      buckets_(nullptr),
      counts_(nullptr),
      size_(0)
{}

Histogram::Histogram::Histogram(const Config *config, const std::string &name)
    : Histogram(*config, name)
{}

Histogram::Histogram::~Histogram() {
    std::stringstream s;
    print(s);
    mlog(HISTOGRAM_NOTE, "\n%s", s.str().c_str());

    clear();
    dealloc_array(cache_);
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
int Histogram::Histogram::add(const double &value) {
    // limit has not been hit
    if (count_ < first_n_) {
        cache_[count_] = value;
    }

    count_++;

    // insert into the buckets if the limit has been hit
    if (count_ >= first_n_) {
        if (!buckets_) {
            // generate the buckets and allocate space for the counts
            if ((gen_(cache_, count_, &buckets_, &size_, extra_) != HISTOGRAM_SUCCESS) ||
                (gen_counts()                                    != HISTOGRAM_SUCCESS)) {
                return HISTOGRAM_ERROR;
            }

            // initialize the upper bound
            buckets_[size_] = std::numeric_limits<double>::min();

            // insert original data
            for(std::size_t i = 0; i < first_n_; i++) {
                insert(cache_[i]);
            }
        }

        // if count == first_n, the value will be in data array
        // cannot use else bcause first_n == 0 will break
        if (count_ > first_n_) {
            insert(value);
        }
    }

    return HISTOGRAM_SUCCESS;
}

/**
 * get_name
 * Gets this histogram's name.
 *
 * @param name      (optional) the name of the histogram
 * @return HISTOGRAM_SUCCESS
 */
int Histogram::Histogram::get_name(std::string &name) const {
    name = name_;
    return HISTOGRAM_SUCCESS;
}

/**
 * get_name
 * Gets this histogram's name.
 *
 * @param name      (optional) the name of the histogram
 * @param name_len  (optional) the length of the histogram's name
 * @return HISTOGRAM_SUCCESS
 */
int Histogram::Histogram::get_name(const char **name, std::size_t *name_len) const {
    if (name) {
        *name = name_.data();
    }

    if (name_len) {
        *name_len = name_.size();
    }

    return HISTOGRAM_SUCCESS;
}

/**
 * get_cache
 * Gets the internal data for generating buckets
 *
 * @param first_n  the maximum number of cached doubles used to generate the buckets
 * @param cache    the array of cached doubles
 * @param size     the number of doubles that have been cached
 * @return HISTOGRAM_SUCCESS
 */
int Histogram::Histogram::get_cache(std::size_t *first_n,
                                    double **cache,
                                    std::size_t *size) const {
    if (first_n) {
        *first_n = first_n_;
    }

    if (cache) {
        *cache = cache_;
    }

    if (size) {
        *size = std::min(first_n_, count_);
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
 * @param buckets   (optional) the buckets of the histogram
 * @param counts    (optional) the counts of the histogram
 * @param size      (optional) how many bucket-count pairs there are
 * @return HISTOGRAM_SUCCESS or HISTOGRAM_ERROR if the buckets have not been generated yet
 */
int Histogram::Histogram::get(double **buckets, std::size_t **counts, std::size_t *size) const {
    if (count_  < first_n_) {
        return HISTOGRAM_ERROR;
    }

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
 * pack_size
 * Get the amount of space needed to pack this Histogram.
 * If the buckets have not been generated yet, the
 * serialized data will stop after size_.
 *
 * @return size
 */
std::size_t Histogram::Histogram::pack_size() const {
    std::size_t total =
        sizeof(name_.size()) + name_.size() +
        sizeof(count_) + sizeof(first_n_) +
        sizeof(size_);

    if (count_ < first_n_) {
        total += sizeof(*cache_) * count_;
    }

    if (size_) {
        total += size_ * (sizeof(*buckets_) + sizeof(*counts_)) +
            sizeof(buckets_[size_]);
    }

    return total;
}

/**
 * pack
 * Serialize a Histogram for transport
 * If the count is less than first_n, pack will fail since
 * the buckets will not have been created yet.
 *
 * @param buf   pointer to a memory location where the serialized data will be
 * @param size  pointer to the size of the serialized data
 * @return true on success, false on error
 */
bool Histogram::Histogram::pack(void **buf, std::size_t *size) const {
    if (!buf || !size) {
        return false;
    }

    *size = pack_size();
    if (!*size) {
        return false;
    }

    *buf = alloc(*size);
    char *curr = (char *) *buf;

    std::size_t avail = *size;
    return pack(curr, avail, nullptr);
}

/**
 * pack
 * Serialize a Histogram in place.
 * Arguments are updated if true was returned.
 *
 * @param curr   the location to start at
 * @param avail  the available memory to use
 * @param used   (optional) the amount of space that was used from the available space
 * @return true on success, false on error
 */
bool Histogram::Histogram::pack(char *&curr, std::size_t &avail, std::size_t *used) const {
    if (!curr || (avail < pack_size())) {
        return false;
    }

    char *orig = curr;

    const std::size_t name_len = name_.size();
    memcpy(curr, &name_len, sizeof(name_len));
    curr += sizeof(name_len);

    memcpy(curr, name_.data(), name_len);
    curr += name_len;

    memcpy(curr, &count_, sizeof(count_));
    curr += sizeof(count_);

    memcpy(curr, &first_n_, sizeof(first_n_));
    curr += sizeof(first_n_);

    if (count_ < first_n_) {
        for(std::size_t i = 0; i < count_; i++) {
            memcpy(curr, &cache_[i], sizeof(cache_[i]));
            curr += sizeof(cache_[i]);
        }
    }

    memcpy(curr, &size_, sizeof(size_));
    curr += sizeof(size_);

    if (size_) {
        for(std::size_t i = 0; i < size_; i++) {
            memcpy(curr, &buckets_[i], sizeof(buckets_[i]));
            curr += sizeof(buckets_[i]);

            memcpy(curr, &counts_[i], sizeof(counts_[i]));
            curr += sizeof(counts_[i]);
        }

        memcpy(curr, &buckets_[size_], sizeof(buckets_[size_]));
        curr += sizeof(buckets_[size_]);
    }

    avail -= curr - orig;

    if (used) {
        *used = curr - orig;
    }

    return true;
}

/**
 * unpack
 * Deserialize Histogram data
 *
 * @param buf  the buffer containing serialized Histogram data
 * @param size size of the buffer
 * @return true on success, false on error
 */
bool Histogram::Histogram::unpack(const void *buf, const std::size_t size) {
    char *curr = (char *) buf;
    std::size_t len = size;
    return unpack(curr, len, nullptr);
}

/**
 * unpack
 * Deserialize Histogram data.
 * Arguments are modified if true is returned
 *
 * @param buf  the buffer containing serialized Histogram data
 * @param size size of the buffer
 * @return true on success, false on error
 */
bool Histogram::Histogram::unpack(char *&curr, std::size_t &size, std::size_t *used) {
    if (!curr ||
        (size < (4 * sizeof(std::size_t)))) {
        return false;
    }

    clear();

    char *orig = curr;

    std::size_t name_len = 0;
    memcpy(&name_len, curr, sizeof(name_len));
    curr += sizeof(name_len);

    name_.assign(curr, name_len);
    curr += name_len;

    memcpy(&count_, curr, sizeof(count_));
    curr += sizeof(count_);

    memcpy(&first_n_, curr, sizeof(first_n_));
    curr += sizeof(first_n_);

    if (count_ < first_n_) {
        dealloc_array(cache_);
        cache_ = alloc_array<double>(first_n_);

        for(std::size_t i = 0; i < count_; i++) {
            memcpy(&cache_[i], curr, sizeof(cache_[i]));
            curr += sizeof(cache_[i]);
        }
    }

    memcpy(&size_, curr, sizeof(size_));
    curr += sizeof(size_);

    if (size_) {
        buckets_ = alloc_array<double>(size_ + 1);
        gen_counts();

        for(std::size_t i = 0; i < size_; i++) {
            memcpy(&buckets_[i], curr, sizeof(buckets_[i]));
            curr += sizeof(buckets_[i]);

            memcpy(&counts_[i], curr, sizeof(counts_[i]));
            curr += sizeof(counts_[i]);
        }

        memcpy(&buckets_[size_], curr, sizeof(buckets_[size_]));
        curr += sizeof(buckets_[size_]);
    }

    if (used) {
        *used = curr - orig;
    }

    return true;
}

/**
 * clear
 * Clears all existing data, including
 * the first n values.
 */
void Histogram::Histogram::clear() {
    first_n_ = 0;
    count_ = 0;

    dealloc_array(buckets_, size_ + 1);
    buckets_ = nullptr;

    dealloc_array(counts_, size_);
    counts_ = nullptr;

    size_ = 0;
}

std::ostream &Histogram::Histogram::print(std::ostream &stream, const std::string &indent) {
    stream << indent << "Histogram has " << count_ << " values" << std::endl;
    if (size_) {
        for(std::size_t i = 0; i < size_; i++) {
            stream << indent << indent << buckets_[i] << ": " << counts_[i] << std::endl;
        }
        stream << indent << indent << buckets_[size_] << std::endl;
    }
    return stream;
}

/**
 * gen_counts
 * Allocates memory for the counts and zeros them
 *
 * @return HISTOGRAM_SUCCESS, or HISTOGRAM_ERROR on error
 */
int Histogram::Histogram::gen_counts() {
    if (count_ < first_n_) {
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
int Histogram::Histogram::insert(const double &value) {
    if (!buckets_ || !counts_ || !size_) {
        return HISTOGRAM_ERROR;
    }

    const std::size_t i = std::upper_bound(buckets_, buckets_ + size_, value) - buckets_ - 1;
    if (i == (std::size_t) -1) {
        return HISTOGRAM_ERROR;
    }

    counts_[i]++;

    // update upper bound
    if (value > buckets_[size_]) {
        buckets_[size_] = value;
    }

    return HISTOGRAM_SUCCESS;
}

void Histogram::deleter(Histogram *hist) {
    destruct(hist);
}
