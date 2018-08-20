#include <gtest/gtest.h>

#include "utils/elapsed.h"

static const time_t sec = 1234;
static const long nsec = 56789;
static struct timespec start;
static struct timespec end;

TEST(elapsed_nano, nsec) {
    start.tv_sec = 0;
    start.tv_nsec = 0;
    end.tv_sec = 0;
    end.tv_nsec = nsec;

    EXPECT_DOUBLE_EQ(nano(start, end), nsec / 1000000000.0);
}

TEST(elapsed_nano, sec) {
    start.tv_sec = 0;
    start.tv_nsec = 0;
    end.tv_sec = sec;
    end.tv_nsec = 0;

    EXPECT_DOUBLE_EQ(nano(start, end), sec);
}

TEST(elapsed_nano, sec_nsec) {
    start.tv_sec = 0;
    start.tv_nsec = 0;
    end.tv_sec = sec;
    end.tv_nsec = nsec;

    EXPECT_DOUBLE_EQ(nano(start, end), sec + nsec / 1000000000.0);
}

TEST(elapsed_nano, same_time) {
    start.tv_sec = sec;
    start.tv_nsec = nsec;
    end.tv_sec = sec;
    end.tv_nsec = nsec;

    EXPECT_DOUBLE_EQ(nano(start, end), 0);
}

TEST(elapsed_nano, double_diff) {
    start.tv_sec = sec;
    start.tv_nsec = nsec;
    end.tv_sec = sec * 2;
    end.tv_nsec = nsec * 2;

    EXPECT_DOUBLE_EQ(nano(start, end), sec + nsec / 1000000000.0);
}
