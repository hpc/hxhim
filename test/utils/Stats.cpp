#include <gtest/gtest.h>

#include "utils/Stats.hpp"

TEST(elapsed, nanoseconds) {
    Timestamp duration;

    // 0 time
    {
        duration.start = Timepoint(std::chrono::nanoseconds(1));
        duration.end = Timepoint(std::chrono::nanoseconds(1));

        EXPECT_EQ(elapsed<std::chrono::nanoseconds>(duration), 0);
    }

    // no nanoseconds
    {
        duration.start = Timepoint(std::chrono::milliseconds(1));
        duration.end = Timepoint(std::chrono::milliseconds(1));

        EXPECT_EQ(elapsed<std::chrono::nanoseconds>(duration), 0);
    }

    // some nanoseconds
    {
        duration.start = Timepoint(std::chrono::nanoseconds(12345));
        duration.end = Timepoint(std::chrono::nanoseconds(54321));

        EXPECT_EQ(elapsed<std::chrono::nanoseconds>(duration), 41976);
    }
}
