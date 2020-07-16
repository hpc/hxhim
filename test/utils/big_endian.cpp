#include <cstring>

#include <gtest/gtest.h>

#include "hxhim/constants.h"
#include "utils/big_endian.hpp"

static const char *SUBJECT = "subject";
static const std::size_t SUBJECT_LEN = strlen(SUBJECT);
static const char *PREDICATE = "predicate";
static const std::size_t PREDICATE_LEN = strlen(PREDICATE);

static const char *SUBJECT_LEN_ENCODED() {
    switch (sizeof(std::size_t)) {
        case 4:
            return "\x00\x00\x00\x07";
        case 8:
            return "\x00\x00\x00\x00\x00\x00\x00\x07";
    }

    return nullptr;
}

static const char *PREDICATE_LEN_ENCODED() {
    switch (sizeof(std::size_t)) {
        case 4:
            return "\x00\x00\x00\x09";
        case 8:
            return "\x00\x00\x00\x00\x00\x00\x00\x09";
    }

    return nullptr;
}

TEST(big_endian, encode) {
    {
        char buf[sizeof(SUBJECT_LEN)] = {0};
        EXPECT_EQ(encode_unsigned(buf, SUBJECT_LEN), HXHIM_SUCCESS);
        EXPECT_EQ(memcmp(SUBJECT_LEN_ENCODED(), buf, sizeof(SUBJECT_LEN)), 0);
    }

    {
        char buf[sizeof(PREDICATE_LEN)] = {0};
        EXPECT_EQ(encode_unsigned(buf, PREDICATE_LEN), HXHIM_SUCCESS);
        EXPECT_EQ(memcmp(PREDICATE_LEN_ENCODED(), buf, sizeof(PREDICATE_LEN)), 0);
    }
}

TEST(big_endian, decode) {
    {
        std::size_t len = 0;
        EXPECT_EQ(decode_unsigned(len, (void *) SUBJECT_LEN_ENCODED()), HXHIM_SUCCESS);
        EXPECT_EQ(len, SUBJECT_LEN);
    }

    {
        std::size_t len = 0;
        EXPECT_EQ(decode_unsigned(len, (void *) PREDICATE_LEN_ENCODED()), HXHIM_SUCCESS);
        EXPECT_EQ(len, PREDICATE_LEN);
    }
}
