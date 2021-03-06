#include <cstring>

#include <gtest/gtest.h>

#include "hxhim/constants.h"
#include "hxhim/triplestore.hpp"
#include "utils/memory.hpp"

static const char *SUBJECT = "subject";
static const std::size_t SUBJECT_LEN = strlen(SUBJECT);
static const char *PREDICATE = "predicate";
static const std::size_t PREDICATE_LEN = strlen(PREDICATE);

static const char *SUBJECT_LEN_ENCODED() {
    switch (sizeof(std::size_t)) {
        case 4:
            return "\x07\x00\x00\x00";
        case 8:
            return "\x07\x00\x00\x00\x00\x00\x00\x00";
    }

    return nullptr;
}

static const char *PREDICATE_LEN_ENCODED() {
    switch (sizeof(std::size_t)) {
        case 4:
            return "\x09\x00\x00\x00";
        case 8:
            return "\x09\x00\x00\x00\x00\x00\x00\x00";
    }

    return nullptr;
}

TEST(triplestore, sp_to_key) {
    Blob sub  = ReferenceBlob((void *) SUBJECT, SUBJECT_LEN, hxhim_data_t::HXHIM_DATA_BYTE);
    Blob pred = ReferenceBlob((void *) PREDICATE, PREDICATE_LEN, hxhim_data_t::HXHIM_DATA_BYTE);

    const std::size_t expected_len = sub.pack_size(false) + pred.pack_size(false);

    std::string key;
    EXPECT_EQ(sp_to_key(sub, pred, key), HXHIM_SUCCESS);
    EXPECT_EQ(key.size(), expected_len);

    const char *curr = key.data();
    EXPECT_EQ(memcmp(curr, SUBJECT, SUBJECT_LEN), 0);

    curr += SUBJECT_LEN;
    EXPECT_EQ(memcmp(curr, SUBJECT_LEN_ENCODED(), sizeof(SUBJECT_LEN)), 0);

    curr += sizeof(SUBJECT_LEN);
    EXPECT_EQ(memcmp(curr, PREDICATE, PREDICATE_LEN), 0);

    curr += PREDICATE_LEN;
    EXPECT_EQ(memcmp(curr, PREDICATE_LEN_ENCODED(), sizeof(PREDICATE_LEN)), 0);
}

TEST(triplestore, key_to_sp) {
    Blob sub  = ReferenceBlob((void *) SUBJECT, SUBJECT_LEN, hxhim_data_t::HXHIM_DATA_BYTE);
    Blob pred = ReferenceBlob((void *) PREDICATE, PREDICATE_LEN, hxhim_data_t::HXHIM_DATA_BYTE);
    const std::size_t expected_len = sub.pack_size(false) + pred.pack_size(false);

    std::string key;
    ASSERT_EQ(sp_to_key(sub, pred, key), HXHIM_SUCCESS);
    EXPECT_EQ(key.size(), expected_len);

    const char *curr = key.data();
    EXPECT_EQ(memcmp(curr, SUBJECT, SUBJECT_LEN), 0);
    curr += SUBJECT_LEN + sizeof(SUBJECT_LEN);
    EXPECT_EQ(memcmp(curr, PREDICATE, PREDICATE_LEN), 0);
    curr += PREDICATE_LEN + sizeof(PREDICATE_LEN);
    EXPECT_EQ(curr, key.data() + key.size());

    // copy
    {
        Blob subject;
        Blob predicate;

        EXPECT_EQ(key_to_sp(key, subject, predicate, true), HXHIM_SUCCESS);

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

        EXPECT_EQ(key_to_sp(key, subject, predicate, false), HXHIM_SUCCESS);

        EXPECT_EQ(subject.data(), key.data());
        EXPECT_EQ(subject.size(), SUBJECT_LEN);
        EXPECT_EQ(memcmp(subject.data(), SUBJECT, subject.size()), 0);

        EXPECT_EQ(predicate.data(), key.data() + subject.size() + sizeof(subject.size()));
        EXPECT_EQ(predicate.size(), PREDICATE_LEN);
        EXPECT_EQ(memcmp(predicate.data(), PREDICATE, predicate.size()), 0);
    }
}
