#ifndef HISTOGRAM_HPP
#define HISTOGRAM_HPP

#include <functional>
#include <map>
#include <list>
#include <set>
#include <type_traits>

const int HISTOGRAM_SUCCESS = 0;
const int HISTOGRAM_ERROR = -1;

/**
 * HistogramBase
 * Base class for histograms
 */
class HistogramBase {
    public:
        virtual ~HistogramBase() = 0;

        virtual int add(const void *value) = 0;
        virtual const std::map<double, std::size_t> &get() const = 0;

        std::size_t added() const {
            return count;
        }

    protected:
        std::size_t count;
};

/**
 * Predefined bucket generation functions
 * Some were taken from https://en.wikipedia.org/wiki/Histogram#Number_of_bins_and_width
 */
template <typename T, typename = std::enable_if_t<std::is_convertible<T, double>::value> >
class HistogramBucketGen {
    public:
        // Alias of function type which takes in a list of values, the histogram variable to fill in, and extra arguments the function needs
        using generator = std::function<int(const std::list<T>&, std::map<double, std::size_t> &, void *)>;

        static int n_buckets                   (const std::list<T> &values, std::map<double, std::size_t> &histogram, void *extra);
        static int square_root_choice          (const std::list<T> &values, std::map<double, std::size_t> &histogram, void *extra);
        static int sturges_formula             (const std::list<T> &values, std::map<double, std::size_t> &histogram, void *extra);
        static int rice_rule                   (const std::list<T> &values, std::map<double, std::size_t> &histogram, void *extra);
        static int scotts_normal_reference_rule(const std::list<T> &values, std::map<double, std::size_t> &histogram, void *extra);
        static int uniform_logn                (const std::list<T> &values, std::map<double, std::size_t> &histogram, void *extra);
};

/**
 * Histogram
 * This class generates histograms using the values added to it.
 */
template <typename T, typename Convert2FP = std::enable_if_t<std::is_convertible<T, double>::value> >
class Histogram : public HistogramBase{
    public:

        Histogram(const std::size_t use_first_n, const typename HistogramBucketGen<T, Convert2FP>::generator &generator, void *extra_args);

        // Add a value to the histogram
        int add(const void *value);
        int add(const T &value);

        // Returns the histogram data as a well known type
        const std::map<double, std::size_t> &get() const;

    private:
        int generate_bins();
        int insert(const T &value);

        std::size_t first_n;                // size of data before the buckets are generated
        typename HistogramBucketGen<T, Convert2FP>::generator gen;
        void *extra;

        std::list <T> data;                 // the data being used to generate the buckets
        std::map<double, std::size_t> hist;
};

#include "Histogram.tpp"

#endif
