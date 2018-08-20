#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

#include <gtest/gtest.h>

#include "utils/Histogram.hpp"

TEST(Histogram, bad_generator) {
    EXPECT_THROW(Histogram::Histogram(10, nullptr, nullptr), std::runtime_error);
}

TEST(Histogram, not_enough_values) {
    const int n = 10;
    Histogram::Histogram h(n + 1, [](const double *, const std::size_t, double **, std::size_t *, void *) { return HISTOGRAM_ERROR; }, nullptr);

    for(std::size_t i = 0; i < n; i++) {
        h.add(i);
    }

    std::size_t size = 0;

    ASSERT_EQ(h.get(nullptr, nullptr, &size), HISTOGRAM_SUCCESS);
    EXPECT_EQ(size, 0);
}

static int every_two(const double *first_n, const std::size_t n, double **buckets, std::size_t *size, void *) {
    if (!first_n || !n    ||
        !buckets || !size) {
        return HISTOGRAM_ERROR;
    }

    // get min and max
    double min = *std::min_element(first_n, first_n + n);
    double max = *std::max_element(first_n, first_n + n);

    *size = std::ceil((max - min) / 2);
    *buckets = new double[*size];

    for(std::size_t i = 0; i < *size; i++) {
        (*buckets)[i] = min;
        min += 2;
    }

    return HISTOGRAM_SUCCESS;
}

TEST(Histogram, every_two) {
    const int n = 10;
    Histogram::Histogram h(n, every_two, nullptr);

    // fill the histogram
    for(std::size_t i = 0; i < n; i++) {
        h.add(i);
    }

    double *buckets = nullptr;
    std::size_t *counts = nullptr;
    std::size_t size = 0;

    ASSERT_EQ(h.get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);
    EXPECT_EQ(size, n / 2);
    for(std::size_t i = 0; i < size; i++) {
        EXPECT_EQ(buckets[i], 2 * i);
        EXPECT_EQ(counts[i], 2);
    }
}

TEST(Histogram, custom_nonuniform) {
    const int n = 10;
    Histogram::Histogram h(n,
                             [](const double *, const std::size_t, double **buckets, std::size_t *size, void *) {

                                 if (!(*buckets = new double[3])) {
                                     return HISTOGRAM_ERROR;
                                 }

                                 (*buckets)[0] = 0;
                                 (*buckets)[1] = 5;
                                 (*buckets)[2] = 9;

                                 *size = 3;

                                 return HISTOGRAM_SUCCESS;
                             },
                             nullptr);

    for(std::size_t i = 0; i < n; i++) {
        h.add(i);
    }

    double *buckets = nullptr;
    std::size_t *counts = nullptr;
    std::size_t size = 0;

    ASSERT_EQ(h.get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);
    EXPECT_EQ(size, 3);

    // 0: 0, 1, 2, 3, 4
    EXPECT_EQ(buckets[0], 0);
    EXPECT_EQ(counts[0], 5);

    // 5: 5, 6, 7, 8
    EXPECT_EQ(buckets[1], 5);
    EXPECT_EQ(counts[1], 4);

    // 9: 9
    EXPECT_EQ(buckets[2], 9);
    EXPECT_EQ(counts[2], 1);
}

TEST(Histogram, uniform_log10) {
    static const std::size_t ten = 10;
    Histogram::Histogram h(ten, histogram_uniform_logn, (void *) &ten);

    for(std::size_t i = 0; i < ten; i++) {
        h.add(i);
    }

    double *buckets = nullptr;
    std::size_t *counts = nullptr;
    std::size_t size = 0;

    ASSERT_EQ(h.get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);

    for(std::size_t i = 0; i < size; i++) {
        EXPECT_EQ(counts[i], 1);
    }

    // Add extra count to last bucket
    h.add(20);

    EXPECT_EQ(counts[size - 1], 2);
}
