#ifndef HISTOGRAM_HPP
#define HISTOGRAM_HPP

#include <functional>
#include <map>
#include <list>
#include <set>
#include <type_traits>

namespace Histogram {

const int SUCCESS = 0;
const int ERROR = -1;

/**
 * Predefined bucket generation functions
 * Some were taken from https://en.wikipedia.org/wiki/Histogram#Number_of_bins_and_width
 */
class BucketGen {
    public:
        // Alias of function type which takes in a list of values, the histogram variable to fill in, and extra arguments the function needs
        typedef std::function<int(const std::list<double> &, std::map<double, std::size_t> &, void *)> generator;

        static int n_buckets                   (const std::list<double> &values, std::map<double, std::size_t> &histogram, void *extra);
        static int square_root_choice          (const std::list<double> &values, std::map<double, std::size_t> &histogram, void *extra);
        static int sturges_formula             (const std::list<double> &values, std::map<double, std::size_t> &histogram, void *extra);
        static int rice_rule                   (const std::list<double> &values, std::map<double, std::size_t> &histogram, void *extra);
        static int scotts_normal_reference_rule(const std::list<double> &values, std::map<double, std::size_t> &histogram, void *extra);
        static int uniform_logn                (const std::list<double> &values, std::map<double, std::size_t> &histogram, void *extra);
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
        // Histogram(const std::map<double, std::size_t> &hist);
        Histogram(const std::size_t use_first_n, const BucketGen::generator &generator, void *extra_args);
        virtual ~Histogram();

        // Add a value to the histogram
        int add(const double &value);

        // Returns the histogram data as a well known type
        const std::map<double, std::size_t> &get() const;

    private:
        int generate_bins();
        int insert(const double &value);

        // set in constructor
        const std::size_t first_n;              // size of data before the buckets are generated
        BucketGen::generator gen;               // the function to use to generate buckets
        void *extra;                            // extra arguments needed by the bucket generator

        // state data
        std::list <double> data;                // the data being used to generate the buckets
        std::map<double, std::size_t> hist;     // the actual histogram
        std::size_t count;                      // the number of values that have been added into this histogram
};

}

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct histogram {
    Histogram::Histogram *histogram;
} histogram_t;

#ifdef __cplusplus
}
#endif

#endif
