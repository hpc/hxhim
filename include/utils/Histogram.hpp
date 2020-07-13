#ifndef HISTOGRAM_HPP
#define HISTOGRAM_HPP

#include <ostream>

#include "utils/memory.hpp"
#include "utils/Histogram.h"

namespace Histogram {

/**
 * Histogram
 * This class generates histograms using the values added to it.
 * Enough values have to be placed into the histogram for the
 * buckets to be generated.
 * Each bucket represents the left (lower) end of a range.
 */
class Histogram {
    public:
        Histogram(const std::size_t use_first_n, const HistogramBucketGenerator_t &generator, void *extra_args);
        virtual ~Histogram();

        // Add a value to the histogram
        int add(const double &value);

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
        std::size_t first_n;                    // size of data before the buckets are generated
        HistogramBucketGenerator_t gen;         // the function to use to generate buckets
        void *extra;                            // extra arguments needed by the bucket generator

        // state data
        double *data;                           // the data being used to generate the buckets
        std::size_t data_size;                  // the number of values that have been added into this histogram

        double *buckets_;                       // the left end of the buckets
        std::size_t *counts_;                   // the counts at the buckets
        std::size_t size_;                      // the number of buckets
};

}

#endif
