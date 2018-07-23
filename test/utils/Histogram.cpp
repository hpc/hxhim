#include <limits>
#include <stdexcept>

#include <gtest/gtest.h>

#include "utils/Histogram.hpp"

TEST(Histogram, bad_generator) {
    EXPECT_THROW(Histogram::Histogram(10, nullptr, nullptr), std::runtime_error);
}

TEST(Histogram, not_enough_values) {
    Histogram::Histogram h(100, [](const std::list<double> &, std::map<double, std::size_t> &histogram, void *) { return Histogram::ERROR; }, nullptr);

    for(std::size_t i = 0; i < 10; i++) {
        h.add(i);
    }

    EXPECT_EQ(h.get().size(), 0);
}

static int every_two(const std::list<double> &values, std::map<double, std::size_t> &histogram, void *) {
    if (!values.size()) {
        return Histogram::ERROR;
    }

    // get min and max
    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::min();
    for(double const &value : values) {
        if (value < min) {
            min = value;
        }

        if (value > max) {
            max = value;
        }
    }

    histogram.clear();

    for(double i = min; i < max; i += 2) {
        histogram[i] = 0;
    }

    return Histogram::SUCCESS;
}

TEST(Histogram, every_two) {
    Histogram::Histogram h(10, every_two, nullptr);

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
    Histogram::Histogram h(10,
                             [](const std::list<double> &, std::map<double, std::size_t> &histogram, void *) {
                                 histogram.clear();

                                 histogram[0] = 0;
                                 histogram[5] = 0;
                                 histogram[9] = 0;

                                 return Histogram::SUCCESS;
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
    Histogram::Histogram h(10, Histogram::BucketGen::uniform_logn, (void *) &ten);

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
