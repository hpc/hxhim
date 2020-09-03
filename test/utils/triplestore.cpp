#include <cstring>

#include <gtest/gtest.h>

#include "hxhim/constants.h"
#include "utils/triplestore.hpp"
#include "utils/memory.hpp"

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

TEST(triplestore, sp_to_key) {
    Blob sub  = ReferenceBlob((void *) SUBJECT, SUBJECT_LEN);
    Blob pred = ReferenceBlob((void *) PREDICATE, PREDICATE_LEN);
    std::size_t buf_len = sub.pack_size() + pred.pack_size();
    char *buf = (char *) alloc(buf_len);
    char *orig = buf;
    std::size_t key_len = 0;

    char *key = sp_to_key(sub, pred, buf, buf_len, key_len);
    EXPECT_EQ(orig + key_len, buf); // the buf pointer moved
    EXPECT_EQ(buf_len, 0);          // used up all available space
    EXPECT_EQ(key, orig);           // the key starts at orig

    char *curr = key;
    EXPECT_EQ(memcmp(curr, SUBJECT, SUBJECT_LEN), 0);

    curr += SUBJECT_LEN;
    EXPECT_EQ(memcmp(curr, SUBJECT_LEN_ENCODED(), sizeof(SUBJECT_LEN)), 0);

    curr += sizeof(SUBJECT_LEN);
    EXPECT_EQ(memcmp(curr, PREDICATE, PREDICATE_LEN), 0);

    curr += PREDICATE_LEN;
    EXPECT_EQ(memcmp(curr, PREDICATE_LEN_ENCODED(), sizeof(PREDICATE_LEN)), 0);

    dealloc(orig);
}

TEST(triplestore, key_to_sp) {
    Blob sub  = ReferenceBlob((void *) SUBJECT, SUBJECT_LEN);
    Blob pred = ReferenceBlob((void *) PREDICATE, PREDICATE_LEN);
    std::size_t buf_len = sub.pack_size() + pred.pack_size();
    char *buf = (char *) alloc(buf_len);
    char *orig = buf;
    std::size_t key_len = 0;

    char *key = sp_to_key(sub, pred, buf, buf_len, key_len);

    char *curr = key;
    EXPECT_EQ(memcmp(curr, SUBJECT, SUBJECT_LEN), 0);
    curr += SUBJECT_LEN + sizeof(SUBJECT_LEN);
    EXPECT_EQ(memcmp(curr, PREDICATE, PREDICATE_LEN), 0);
    curr += PREDICATE_LEN + sizeof(PREDICATE_LEN);
    EXPECT_EQ(curr, ((char *) key) + key_len);

    // copy
    {
        Blob subject;
        Blob predicate;

        EXPECT_EQ(key_to_sp(key, key_len, subject, predicate, true), HXHIM_SUCCESS);

        EXPECT_NE(subject.data(), SUBJECT);
        EXPECT_EQ(subject.size(), SUBJECT_LEN);
        EXPECT_EQ(memcmp(subject.data(), SUBJECT, subject.size()), 0);

        EXPECT_NE(predicate.data(), PREDICATE);
        EXPECT_EQ(predicate.size(), PREDICATE_LEN);
        EXPECT_EQ(memcmp(predicate.data(), PREDICATE, predicate.size()), 0);
    }

    // reference
    {
        Blob subject;
        Blob predicate;

        EXPECT_EQ(key_to_sp(key, key_len, subject, predicate, false), HXHIM_SUCCESS);

        EXPECT_EQ(subject.data(), key);
        EXPECT_EQ(subject.size(), SUBJECT_LEN);
        EXPECT_EQ(memcmp(subject.data(), SUBJECT, subject.size()), 0);

        EXPECT_EQ(predicate.data(), ((char *) key) + subject.size() + sizeof(subject.size()));
        EXPECT_EQ(predicate.size(), PREDICATE_LEN);
        EXPECT_EQ(memcmp(predicate.data(), PREDICATE, predicate.size()), 0);
    }

    dealloc(orig);
}
