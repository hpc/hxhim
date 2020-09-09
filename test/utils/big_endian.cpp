#include <cstring>

#include <gtest/gtest.h>

#include "hxhim/constants.h"
#include "utils/big_endian.hpp"

// char arrays stay the same
TEST(big_endian, c_string) {
    const std::size_t len = (rand() % 32) + 1;
    char *orig = new char[len]();
    for(std::size_t i = 0; i < len; i++){
        orig[i] = rand();
    }

    {
        char *encoded = new char[len]();
        EXPECT_EQ(big_endian::encode(encoded, orig, len), HXHIM_SUCCESS);
        EXPECT_EQ(memcmp(encoded, orig, len), 0);
        delete [] encoded;
    }

    {
        char *decoded = new char[len]();
        EXPECT_EQ(big_endian::decode(decoded, orig, len), HXHIM_SUCCESS);
        EXPECT_EQ(memcmp(decoded, orig, len), 0);
        delete [] decoded;
    }

    delete [] orig;
}

TEST(big_endian, int) {
    const int orig = rand();

    char encoded[sizeof(orig)] = {};
    EXPECT_EQ(big_endian::encode(encoded, orig, sizeof(orig)), HXHIM_SUCCESS);

    int decoded = 0;
    EXPECT_EQ(big_endian::decode(decoded, encoded, sizeof(orig)), HXHIM_SUCCESS);
    EXPECT_EQ(decoded, orig);
}

TEST(big_endian, float) {
    const float orig = rand() + rand() / rand();

    char encoded[sizeof(orig)] = {};
    EXPECT_EQ(big_endian::encode(encoded, orig, sizeof(orig)), HXHIM_SUCCESS);

    float decoded = 0;
    EXPECT_EQ(big_endian::decode(decoded, encoded, sizeof(orig)), HXHIM_SUCCESS);
    EXPECT_EQ(decoded, orig);
}

TEST(big_endian, double) {
    const double orig = rand() + rand() / rand();

    char encoded[sizeof(orig)] = {};
    EXPECT_EQ(big_endian::encode(encoded, orig, sizeof(orig)), HXHIM_SUCCESS);

    double decoded = 0;
    EXPECT_EQ(big_endian::decode(decoded, encoded, sizeof(orig)), HXHIM_SUCCESS);
    EXPECT_EQ(decoded, orig);
}

TEST(big_endian, Enum) {

    enum TestEnum {};

    const TestEnum orig = (TestEnum) rand();

    char encoded[sizeof(Test)] = {};
    EXPECT_EQ(big_endian::encode(encoded, orig, sizeof(orig)), HXHIM_SUCCESS);

    TestEnum decoded = (TestEnum) 0;
    EXPECT_EQ(big_endian::decode(decoded, encoded, sizeof(orig)), HXHIM_SUCCESS);
    EXPECT_EQ(decoded, orig);
}
