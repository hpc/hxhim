#include <cstring>

#include <gtest/gtest.h>

#include "hxhim/triplestore.hpp"

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

TEST(triplestore, encode_unsigned) {
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

TEST(triplestore, decode_unsigned) {
    {
        std::size_t len = 0;
        EXPECT_EQ(decode_unsigned((void *) SUBJECT_LEN_ENCODED(), len), HXHIM_SUCCESS);
        EXPECT_EQ(len, SUBJECT_LEN);
    }

    {
        std::size_t len = 0;
        EXPECT_EQ(decode_unsigned((void *) PREDICATE_LEN_ENCODED(), len), HXHIM_SUCCESS);
        EXPECT_EQ(len, PREDICATE_LEN);
    }
}

TEST(triplestore, sp_to_key) {
    FixedBufferPool *fbp = new FixedBufferPool(SUBJECT_LEN + sizeof(std::size_t) +
                                               PREDICATE_LEN + sizeof(std::size_t), 1);
    ASSERT_NE(fbp, nullptr);

    void *key = nullptr;
    std::size_t key_len;

    EXPECT_EQ(sp_to_key(fbp, SUBJECT, SUBJECT_LEN, PREDICATE, PREDICATE_LEN, &key, &key_len), HXHIM_SUCCESS);

    char *curr = (char *) key;
    EXPECT_EQ(memcmp(curr, SUBJECT, SUBJECT_LEN), 0);

    curr += SUBJECT_LEN;
    EXPECT_EQ(memcmp(curr, SUBJECT_LEN_ENCODED(), sizeof(SUBJECT_LEN)), 0);

    curr += sizeof(SUBJECT_LEN);
    EXPECT_EQ(memcmp(curr, PREDICATE, PREDICATE_LEN), 0);

    curr += PREDICATE_LEN;
    EXPECT_EQ(memcmp(curr, PREDICATE_LEN_ENCODED(), sizeof(PREDICATE_LEN)), 0);

    fbp->release(key);

    delete fbp;
}

TEST(triplestore, key_to_sp) {
    FixedBufferPool *fbp = new FixedBufferPool(SUBJECT_LEN + sizeof(std::size_t) +
                                               PREDICATE_LEN + sizeof(std::size_t), 1);
    ASSERT_NE(fbp, nullptr);

    void *key = nullptr;
    std::size_t key_len;

    EXPECT_EQ(sp_to_key(fbp, SUBJECT, SUBJECT_LEN, PREDICATE, PREDICATE_LEN, &key, &key_len), HXHIM_SUCCESS);

    void *subject = nullptr;
    std::size_t subject_len = 0;
    void *predicate = nullptr;
    std::size_t predicate_len = 0;

    EXPECT_EQ(key_to_sp(key, key_len, &subject, &subject_len, &predicate, &predicate_len), HXHIM_SUCCESS);
    EXPECT_EQ(subject_len, SUBJECT_LEN);
    EXPECT_EQ(memcmp(subject, SUBJECT, subject_len), 0);
    EXPECT_EQ(predicate_len, PREDICATE_LEN);
    EXPECT_EQ(memcmp(predicate, PREDICATE, predicate_len), 0);

    fbp->release(key);

    delete fbp;
}
