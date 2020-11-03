#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

#include <gtest/gtest.h>

#include "TestHistogram.hpp"
#include "utils/Histogram.hpp"

TEST(Histogram, not_enough_values) {
    const int n = 10;
    Histogram::Histogram h({n + 1, [](const double *, const std::size_t, double **, std::size_t *, void *) { return HISTOGRAM_ERROR; }, nullptr});

    for(std::size_t i = 0; i < n; i++) {
        h.add(i);
    }

    ASSERT_EQ(h.get(nullptr, nullptr, nullptr), HISTOGRAM_ERROR);
}

TEST(Histogram, add) {
    const std::size_t FIRST_N = 5;

    Histogram::Histogram hist({FIRST_N,
                               [](const double *, const size_t,
                                  double **buckets, size_t *size,
                                  void *) -> int {
                                   if (!buckets || !size) {
                                       return HISTOGRAM_ERROR;
                                   }

                                   *size = 1;
                                   *buckets = alloc_array<double>(*size);
                                   (*buckets)[0] = 0;

                                   return HISTOGRAM_SUCCESS;
                               },
                               nullptr});

    std::size_t first_n = 0;
    double *cache = nullptr;
    std::size_t cache_size = 0;

    for(std::size_t i = 0; i < FIRST_N; i++) {
        // check before inserting
        EXPECT_EQ(hist.get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
        EXPECT_EQ(first_n, FIRST_N);
        EXPECT_NE(cache, nullptr);
        EXPECT_EQ(cache_size, i);

        EXPECT_EQ(hist.get(nullptr, nullptr, nullptr), HISTOGRAM_ERROR);

        hist.add(0);
    }

    EXPECT_EQ(hist.get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
    EXPECT_EQ(first_n, FIRST_N);
    EXPECT_NE(cache, nullptr);
    EXPECT_EQ(cache_size, FIRST_N);

    // add an extra value
    hist.add(0);

    EXPECT_EQ(hist.get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
    EXPECT_EQ(first_n, FIRST_N);
    EXPECT_NE(cache, nullptr);
    EXPECT_EQ(cache_size, FIRST_N); // FIRST_N + 1 values, but only FIRST_N values cached

    // check contents
    double *buckets = nullptr;
    std::size_t *counts = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(hist.get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);
    EXPECT_NE(buckets, nullptr);
    EXPECT_NE(counts, nullptr);
    EXPECT_EQ(size, 1);
    EXPECT_EQ(buckets[0], 0);
    EXPECT_EQ(counts[0], FIRST_N + 1);
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
    *buckets = alloc_array<double>(*size);

    for(std::size_t i = 0; i < *size; i++) {
        (*buckets)[i] = min;
        min += 2;
    }

    return HISTOGRAM_SUCCESS;
}

TEST(Histogram, every_two) {
    const std::size_t n = 10;
    Histogram::Histogram h({n, every_two, nullptr});

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
        EXPECT_EQ(buckets[i], (std::size_t) (2 * i));
        EXPECT_EQ(counts[i], (std::size_t) 2);
    }
}

TEST(Histogram, uniform_log10) {
    static const std::size_t ten = 10;
    Histogram::Histogram h({ten, histogram_uniform_logn, (void *) (uintptr_t) ten});
    for(std::size_t i = 0; i < ten; i++) {
        h.add(i);
    }

    double *buckets = nullptr;
    std::size_t *counts = nullptr;
    std::size_t size = 0;

    ASSERT_EQ(h.get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);

    for(std::size_t i = 0; i < size; i++) {
        EXPECT_EQ(counts[i], (std::size_t) 1);
    }

    // Add extra count to last bucket
    h.add(20);

    EXPECT_EQ(counts[size - 1], (std::size_t) 2);
}

TEST(Histogram, custom_nonuniform) {
    CUSTOM_NONUNIFORM_INIT(h, 10);
    CUSTOM_NONUNIFORM_FILL(h, 10);
    CUSTOM_NONUNIFORM_TEST(h);
}

TEST(Histogram, pack_unpack) {
    // first_n == 0
    {
        CUSTOM_NONUNIFORM_INIT(src, 00);
        CUSTOM_NONUNIFORM_FILL(src, 10);

        void *buf = nullptr;
        std::size_t size = 0;
        EXPECT_EQ(src.pack(&buf, &size), true);
        ASSERT_NE(buf, nullptr);

        Histogram::Histogram dst({0, nullptr, nullptr});
        EXPECT_EQ(dst.unpack(buf, size), true);

        dealloc(buf);

        CUSTOM_NONUNIFORM_TEST(dst);
    }

    // first_n != 0 not enough data
    {
        CUSTOM_NONUNIFORM_INIT(src, 10);

        void *buf = nullptr;
        std::size_t size = 0;
        EXPECT_EQ(src.pack(&buf, &size), false);
        EXPECT_EQ(buf, nullptr);
        EXPECT_EQ(size, 0);
    }

    // good pack/unpack
    {
        CUSTOM_NONUNIFORM_INIT(src, 10);
        CUSTOM_NONUNIFORM_FILL(src, 10);

        void *buf = nullptr;
        std::size_t size = 0;
        EXPECT_EQ(src.pack(&buf, &size), true);
        ASSERT_NE(buf, nullptr);

        Histogram::Histogram dst({0, nullptr, nullptr});
        EXPECT_EQ(dst.unpack(buf, size), true);

        dealloc(buf);

        CUSTOM_NONUNIFORM_TEST(dst);
    }

    // bad unpack
    {
        Histogram::Histogram dst({0, nullptr, nullptr});

        // invalid pointer
        EXPECT_EQ(dst.unpack(nullptr, 2 * sizeof(std::size_t)), false);

        // "valid" pointer but not large enough
        EXPECT_EQ(dst.unpack((void *) (uintptr_t) 1,
                             2 * sizeof(std::size_t) - 1), false);
    }
}
