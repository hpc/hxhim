#include <algorithm>
#include <cmath>

#include <gtest/gtest.h>

#include "utils/Histogram.hpp"

static const std::string TEST_HIST_NAME = "Test Histogram Name";

int CUSTOM_NONUNIFORM_FUNC(const double *, const size_t,
                           double **buckets, size_t *size,
                           void *) {
    *size = 3;

    *buckets = alloc_array<double>(*size + 1);

    (*buckets)[0] = 0;
    (*buckets)[1] = 5;
    (*buckets)[2] = 9;
    (*buckets)[*size] = -1;

    return HISTOGRAM_SUCCESS;
}

#define CUSTOM_NONUNIFORM_INIT(name, count)                             \
    const int n = count;                                                \
    ::Histogram::Histogram name(Histogram::Config{n,                    \
                                CUSTOM_NONUNIFORM_FUNC,                 \
                                nullptr}, TEST_HIST_NAME)

#define CUSTOM_NONUNIFORM_FILL(name, count)     \
    for(std::size_t i = 0; i < (count); i++) {  \
        name.add(i);                            \
    }

#define CUSTOM_NONUNIFORM_TEST(name)                                       \
    {                                                                      \
        const char *name_str = nullptr;                                    \
        std::size_t name_len = 0;                                          \
        EXPECT_EQ(name.get_name(&name_str, &name_len), HISTOGRAM_SUCCESS); \
        EXPECT_EQ(std::string(name_str, name_len), TEST_HIST_NAME);        \
                                                                           \
        double *buckets = nullptr;                                         \
        std::size_t *counts = nullptr;                                     \
        std::size_t size = 0;                                              \
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
                                                                           \
        EXPECT_EQ(buckets[3], (double) 9);                                 \
    }


TEST(Histogram, not_enough_values) {
    const int n = 10;
    Histogram::Histogram h({n + 1,
                [](const double *, const std::size_t, double **, std::size_t *, void *) {
                    return HISTOGRAM_ERROR;
                },
                nullptr}, TEST_HIST_NAME);

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
                                   *buckets = alloc_array<double>(*size + 1);
                                   (*buckets)[0] = 0;

                                   return HISTOGRAM_SUCCESS;
                               },
                               nullptr}, TEST_HIST_NAME);

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
    const char *name = nullptr;
    std::size_t name_len = 0;
    EXPECT_EQ(hist.get_name(&name, &name_len), HISTOGRAM_SUCCESS);
    EXPECT_EQ(std::string(name, name_len), TEST_HIST_NAME);

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
    *buckets = alloc_array<double>(*size + 1);

    for(std::size_t i = 0; i < *size; i++) {
        (*buckets)[i] = min;
        min += 2;
    }

    return HISTOGRAM_SUCCESS;
}

TEST(Histogram, every_two) {
    const std::size_t n = 10;
    Histogram::Histogram h({n, every_two, nullptr}, TEST_HIST_NAME);

    // fill the histogram
    for(std::size_t i = 0; i < n; i++) {
        h.add(i);
    }

    const char *name = nullptr;
    std::size_t name_len = 0;
    EXPECT_EQ(h.get_name(&name, &name_len), HISTOGRAM_SUCCESS);
    EXPECT_EQ(std::string(name, name_len), TEST_HIST_NAME);

    double *buckets = nullptr;
    std::size_t *counts = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(h.get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);
    EXPECT_EQ(size, n / 2);
    for(std::size_t i = 0; i < size; i++) {
        EXPECT_EQ(buckets[i], (std::size_t) (2 * i));
        EXPECT_EQ(counts[i], (std::size_t) 2);
    }
}

TEST(Histogram, uniform_log10) {
    static const std::size_t ten = 10;
    Histogram::Histogram h({ten, histogram_uniform_logn, (void *) (uintptr_t) ten}, TEST_HIST_NAME);
    for(std::size_t i = 0; i < ten; i++) {
        h.add(i);
    }

    const char *name = nullptr;
    std::size_t name_len = 0;
    EXPECT_EQ(h.get_name(&name, &name_len), HISTOGRAM_SUCCESS);
    EXPECT_EQ(std::string(name, name_len), TEST_HIST_NAME);

    double *buckets = nullptr;
    std::size_t *counts = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(h.get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);

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

        Histogram::Histogram dst({0, nullptr, nullptr}, TEST_HIST_NAME);
        EXPECT_EQ(dst.unpack(buf, size), true);

        dealloc(buf);

        CUSTOM_NONUNIFORM_TEST(dst);
    }

    // buckets haven't been generated yet
    {
        CUSTOM_NONUNIFORM_INIT(src, 10);

        void *buf = nullptr;
        std::size_t size = 0;
        EXPECT_EQ(src.pack(&buf, &size), true);
        EXPECT_NE(buf, nullptr);
        EXPECT_EQ(size,
                  TEST_HIST_NAME.size() + sizeof(TEST_HIST_NAME.size()) +
                  3 * sizeof(std::size_t));

        dealloc(buf);
    }

    // buckets have been generated
    {
        CUSTOM_NONUNIFORM_INIT(src, 10);
        CUSTOM_NONUNIFORM_FILL(src, 10);

        void *buf = nullptr;
        std::size_t size = 0;
        EXPECT_EQ(src.pack(&buf, &size), true);
        ASSERT_NE(buf, nullptr);

        Histogram::Histogram dst({0, nullptr, nullptr}, TEST_HIST_NAME);
        EXPECT_EQ(dst.unpack(buf, size), true);

        dealloc(buf);

        CUSTOM_NONUNIFORM_TEST(dst);
    }

    // bad unpack
    {
        Histogram::Histogram dst({0, nullptr, nullptr}, TEST_HIST_NAME);

        // invalid pointer
        EXPECT_EQ(dst.unpack(nullptr, 2 * sizeof(std::size_t)), false);

        // "valid" pointer but not large enough
        EXPECT_EQ(dst.unpack((void *) (uintptr_t) 1,
                             2 * sizeof(std::size_t) - 1), false);
    }
}
