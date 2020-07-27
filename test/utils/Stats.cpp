#include <gtest/gtest.h>

#include "utils/Stats.hpp"

TEST(Stats, chrono) {
    ::Stats::Chronostamp duration;

    // 0 time
    {
        duration.start = ::Stats::Chronopoint(std::chrono::nanoseconds(rand()));
        duration.end = duration.start;

        EXPECT_EQ(::Stats::nano(duration), 0);
    }

    // no nanoseconds
    {
        duration.start = ::Stats::Chronopoint(std::chrono::milliseconds(rand()));
        duration.end = duration.start;

        EXPECT_EQ(::Stats::nano(duration), 0);
    }

    // some nanoseconds
    {
        duration.start = ::Stats::Chronopoint(std::chrono::nanoseconds(12345));
        duration.end = ::Stats::Chronopoint(std::chrono::nanoseconds(54321));

        EXPECT_EQ(::Stats::nano(duration), 41976);
    }
}
