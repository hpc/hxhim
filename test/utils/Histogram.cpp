#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

#include <gtest/gtest.h>

#include "utils/Histogram.hpp"

TEST(Histogram, not_enough_values) {
    const int n = 10;
    Histogram::Histogram h(n + 1, [](const double *, const std::size_t, double **, std::size_t *, void *) { return HISTOGRAM_ERROR; }, nullptr);

    for(std::size_t i = 0; i < n; i++) {
        h.add(i);
    }

    std::size_t size = 0;

    ASSERT_EQ(h.get(nullptr, nullptr, &size), HISTOGRAM_SUCCESS);
    EXPECT_EQ(size, (std::size_t) 0);
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
    EXPECT_EQ(size, (std::size_t) (n / 2));
    for(std::size_t i = 0; i < size; i++) {
        EXPECT_EQ(buckets[i], (std::size_t) (2 * i));
        EXPECT_EQ(counts[i], (std::size_t) 2);
    }
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
        EXPECT_EQ(counts[i], (std::size_t) 1);
    }

    // Add extra count to last bucket
    h.add(20);

    EXPECT_EQ(counts[size - 1], (std::size_t) 2);
}

#define CUSTOM_NONUNIFORM_INIT(name, count)                                   \
    const int n = count;                                                      \
    ::Histogram::Histogram name(n,                                            \
                              [](const double *, const std::size_t,           \
                                 double **buckets, std::size_t *size,         \
                                 void *) {                                    \
                                  if (!(*buckets = alloc_array<double>(3))) { \
                                      return HISTOGRAM_ERROR;                 \
                                  }                                           \
                                                                              \
                                  (*buckets)[0] = 0;                          \
                                  (*buckets)[1] = 5;                          \
                                  (*buckets)[2] = 9;                          \
                                                                              \
                                  *size = 3;                                  \
                                                                              \
                                  return HISTOGRAM_SUCCESS;                   \
                              },                                              \
                              nullptr)

#define CUSTOM_NONUNIFORM_FILL(name, count)     \
    for(std::size_t i = 0; i < (count); i++) {  \
        name.add(i);                            \
    }

#define CUSTOM_NONUNIFORM_TEST(name)                                       \
    {                                                                      \
        double *buckets = nullptr;                                         \
        std::size_t *counts = nullptr;                                     \
        std::size_t size = 0;                                              \
                                                                           \
        ASSERT_EQ(name.get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);  \
        EXPECT_EQ(size, (std::size_t) 3);                                  \
                                                                           \
        /* 0: 0, 1, 2, 3, 4 */                                             \
        EXPECT_EQ(buckets[0], (double) 0);                                 \
        EXPECT_EQ(counts[0], (std::size_t) 5);                             \
                                                                           \
        /* 5: 5, 6, 7, 8 */                                                \
        EXPECT_EQ(buckets[1], (double) 5);                                 \
        EXPECT_EQ(counts[1], (std::size_t) 4);                             \
                                                                           \
        /* 9: 9 */                                                         \
        EXPECT_EQ(buckets[2], (double) 9);                                 \
        EXPECT_EQ(counts[2], (std::size_t) 1);                             \
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

        Histogram::Histogram dst(0, nullptr, nullptr);
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

        Histogram::Histogram dst(0, nullptr, nullptr);
        EXPECT_EQ(dst.unpack(buf, size), true);

        dealloc(buf);

        CUSTOM_NONUNIFORM_TEST(dst);
    }

    // bad unpack
    {
        Histogram::Histogram dst(0, nullptr, nullptr);

        // invalid pointer
        EXPECT_EQ(dst.unpack(nullptr, 2 * sizeof(std::size_t)), false);

        // "valid" pointer but not large enough
        EXPECT_EQ(dst.unpack((void *) (uintptr_t) 1,
                             2 * sizeof(std::size_t) - 1), false);
    }
}
