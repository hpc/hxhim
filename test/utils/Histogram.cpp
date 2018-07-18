#include <limits>
#include <stdexcept>

#include <gtest/gtest.h>

#include "utils/Histogram.hpp"

TEST(Histogram, bad_generator) {
    EXPECT_THROW(Histogram<std::size_t>(10, nullptr, nullptr), std::runtime_error);
}

TEST(Histogram, not_enough_values) {
    Histogram<std::size_t> h(100, [](const std::list<std::size_t> &, std::map<double, std::size_t> &histogram, void *) { return HISTOGRAM_ERROR; }, nullptr);

    for(std::size_t i = 0; i < 10; i++) {
        h.add(i);
    }

    EXPECT_EQ(h.get().size(), 0);
}

static int every_two(const std::list<std::size_t> &values, std::map<double, std::size_t> &histogram, void *) {
    if (!values.size()) {
        return HISTOGRAM_ERROR;
    }

    // get min and max
    std::size_t min = std::numeric_limits<std::size_t>::max();
    std::size_t max = std::numeric_limits<std::size_t>::min();
    for(std::size_t const &value : values) {
        if (value < min) {
            min = value;
        }

        if (value > max) {
            max = value;
        }
    }

    histogram.clear();

    for(std::size_t i = min; i < max; i += 2) {
        histogram[i] = 0;
    }

    return HISTOGRAM_SUCCESS;
}

TEST(Histogram, every_two) {
    Histogram<std::size_t> h(10, every_two, nullptr);

    // fill the histogram
    for(std::size_t i = 0; i < 10; i++) {
        h.add(i);
    }

    EXPECT_EQ(h.get().size(), 5);
    for(std::pair<const double, std::size_t> const &bin : h.get()) {
        EXPECT_EQ(bin.second, 2);
    }
}

TEST(Histogram, custom_nonuniform) {
    Histogram<std::size_t> h(10,
                             [](const std::list<std::size_t> &, std::map<double, std::size_t> &histogram, void *) {
                                 histogram.clear();

                                 histogram[0] = 0;
                                 histogram[5] = 0;
                                 histogram[9] = 0;

                                 return HISTOGRAM_SUCCESS;
                             },
                             nullptr);

    for(std::size_t i = 0; i < 10; i++) {
        h.add(i);
    }

    EXPECT_EQ(h.get().at(0), 5); // 0, 1, 2, 3, 4
    EXPECT_EQ(h.get().at(5), 4); // 5, 6, 7, 8
    EXPECT_EQ(h.get().at(9), 1); // 9
}

TEST(Histogram, uniform_log10) {
    static const std::size_t ten = 10;
    Histogram<double> h(10, HistogramBucketGen<double>::uniform_logn, (void *) &ten);

    for(std::size_t i = 0; i < 10; i++) {
        h.add(i);
    }

    for(std::pair<const double, std::size_t> const &bin : h.get()) {
        EXPECT_EQ(bin.second, 1);
    }

    // Add extra count to last bucket
    h.add(20);

    EXPECT_EQ(h.get().rbegin()->second, 2);
}
