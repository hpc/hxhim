#ifndef TEST_HISTOGRAM_HPP
#define TEST_HISTOGRAM_HPP

#include "utils/Histogram.hpp"

int CUSTOM_NONUNIFORM_FUNC(const double *first_n, const size_t n,
                           double **buckets, size_t *size,
                           void *extra);

#define CUSTOM_NONUNIFORM_INIT(name, count)                             \
    const int n = count;                                                \
    ::Histogram::Histogram name(Histogram::Config{n,                    \
                                CUSTOM_NONUNIFORM_FUNC,                 \
                                nullptr})

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

#endif
