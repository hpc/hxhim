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
    ReferenceBlob sub((void *) SUBJECT, SUBJECT_LEN);
    ReferenceBlob pred((void *) PREDICATE, PREDICATE_LEN);
    void *key = nullptr;
    std::size_t key_len;

    EXPECT_EQ(sp_to_key(sub, pred, &key, &key_len), HXHIM_SUCCESS);

    char *curr = (char *) key;
    EXPECT_EQ(memcmp(curr, SUBJECT, SUBJECT_LEN), 0);

    curr += SUBJECT_LEN;
    EXPECT_EQ(memcmp(curr, SUBJECT_LEN_ENCODED(), sizeof(SUBJECT_LEN)), 0);

    curr += sizeof(SUBJECT_LEN);
    EXPECT_EQ(memcmp(curr, PREDICATE, PREDICATE_LEN), 0);

    curr += PREDICATE_LEN;
    EXPECT_EQ(memcmp(curr, PREDICATE_LEN_ENCODED(), sizeof(PREDICATE_LEN)), 0);

    dealloc(key);
}

TEST(triplestore, key_to_sp) {
    ReferenceBlob sub((void *) SUBJECT, SUBJECT_LEN);
    ReferenceBlob pred((void *) PREDICATE, PREDICATE_LEN);
    void *key = nullptr;
    std::size_t key_len;

    EXPECT_EQ(sp_to_key(&sub, &pred, &key, &key_len), HXHIM_SUCCESS);

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

    dealloc(key);
}
