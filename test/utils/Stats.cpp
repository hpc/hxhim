#include <gtest/gtest.h>

#include "utils/Stats.hpp"

TEST(Stats, ctime) {
    Monostamp duration;

    // seconds only
    {
        duration.start.tv_sec  = rand();
        duration.start.tv_nsec = 0;
        duration.end.tv_sec    = duration.start.tv_sec;
        duration.end.tv_nsec   = 0;

        EXPECT_EQ(elapsed(&duration), 0);
    }

    // nanoseconds only
    {
        duration.start.tv_sec  = 0;
        duration.start.tv_nsec = rand();
        duration.end.tv_sec    = 0;
        duration.end.tv_nsec   = duration.start.tv_nsec;

        EXPECT_EQ(elapsed(&duration), 0);
    }

    // both
    {
        duration.start.tv_sec  = rand();
        duration.start.tv_nsec = rand();
        duration.end.tv_sec    = duration.start.tv_sec;
        duration.end.tv_nsec   = duration.start.tv_nsec;

        EXPECT_EQ(elapsed(&duration), 0);
    }
}

TEST(Stats, chrono) {
    Chronostamp duration;

    // 0 time
    {
        duration.start = Chronopoint(std::chrono::nanoseconds(rand()));
        duration.end = duration.start;

        EXPECT_EQ(elapsed<std::chrono::nanoseconds>(duration), 0);
    }

    // no nanoseconds
    {
        duration.start = Chronopoint(std::chrono::milliseconds(rand()));
        duration.end = duration.start;

        EXPECT_EQ(elapsed<std::chrono::nanoseconds>(duration), 0);
    }

    // some nanoseconds
    {
        duration.start = Chronopoint(std::chrono::nanoseconds(12345));
        duration.end = Chronopoint(std::chrono::nanoseconds(54321));

        EXPECT_EQ(elapsed<std::chrono::nanoseconds>(duration), 41976);
    }
}
