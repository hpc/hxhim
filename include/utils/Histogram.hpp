#ifndef HISTOGRAM_HPP
#define HISTOGRAM_HPP

#include <ostream>

#include "utils/memory.hpp"
#include "utils/Histogram.h"

namespace Histogram {

struct Config {
    std::size_t first_n;
    HistogramBucketGenerator_t generator;
    void *extra_args;
};

/**
 * Histogram
 * This class generates histograms using the values added to it.
 * Enough values have to be placed into the histogram for the
 * buckets to be generated.
 * Each bucket represents the left (lower) end of a range.
 */
class Histogram {
    public:
        Histogram();
        Histogram(const Config &config, const std::string &name);
        Histogram(const Config *config, const std::string &name);
        virtual ~Histogram();

        // Add a value to the histogram
        int add(const double &value);

        int get_name(std::string &name) const;
        int get_name(const char **name, std::size_t *name_len) const;

        // Return references to the data being used to generate the buckets
        int get_cache(std::size_t *first_n, double **cache, std::size_t *size) const;

        // Return references to internal arrays
        int get(double **buckets, std::size_t **counts, std::size_t *size) const;

        std::size_t pack_size() const;
        bool pack(void **buf, std::size_t *size) const;
        bool pack(char *&curr, std::size_t &avail, std::size_t *used) const;
        bool unpack(const void *buf, const std::size_t size);
        bool unpack(char *&curr, std::size_t &size, std::size_t *used);

        // removes all datapoints, including the first_n values
        void clear();

        std::ostream &print(std::ostream &stream, const std::string &indent = "    ");

    private:
        int gen_counts();
        int insert(const double &value);

        // set in constructor
        std::string name_;
        std::size_t first_n_;                   // size of data before the buckets are generated
        HistogramBucketGenerator_t gen_;        // the function to use to generate buckets
        void *extra_;                           // extra arguments needed by the bucket generator

        // state data
        double *cache_;                         // the data being used to generate the buckets
        std::size_t count_;                     // the number of values that have been added into this histogram; also used for generating buckets

        double *buckets_;                       // the left end of the buckets (has size_ + 1 values)
        std::size_t *counts_;                   // the counts at the buckets
        std::size_t size_;                      // the number of buckets
};

// deleter for std::shared_ptr
void deleter(Histogram *hist);

}

#endif
