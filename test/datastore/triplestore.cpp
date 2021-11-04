#include <string>

#include <gtest/gtest.h>

#include "hxhim/constants.h"
#include "datastore/triplestore.hpp"
#include "utils/memory.hpp"

static const std::string SUBJECT   = "subject";
static const std::string PREDICATE = "predicate";
static const std::string OBJECT    = "object";

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
    Blob sub(SUBJECT);
    Blob pred(PREDICATE);

    const std::size_t expected_len = sub.pack_size(false) +
                                     pred.pack_size(false) +
                                     3 * sizeof(char);

    Blob key;
    EXPECT_EQ(sp_to_key(sub, pred, &key), HXHIM_SUCCESS);
    EXPECT_EQ(key.size(), expected_len);

    const char *curr = (char *) key.data();

    // subject
    EXPECT_EQ(memcmp(curr, SUBJECT.data(), SUBJECT.size()), 0);
    curr += SUBJECT.size();

    // predicate
    EXPECT_EQ(memcmp(curr, PREDICATE.data(), PREDICATE.size()), 0);
    curr += PREDICATE.size();

    EXPECT_EQ(*curr, '\x0ff');
    curr++;

    // subject length
    EXPECT_EQ(memcmp(curr, SUBJECT_LEN_ENCODED(), sizeof(SUBJECT.size())), 0);
    curr += sizeof(SUBJECT.size());

    // predicate length
    EXPECT_EQ(memcmp(curr, PREDICATE_LEN_ENCODED(), sizeof(PREDICATE.size())), 0);
    curr += sizeof(PREDICATE.size());

    // subject type
    EXPECT_EQ(*curr, sub.data_type());
    curr++;

    // predicate type
    EXPECT_EQ(*curr, pred.data_type());
    curr++;

    ASSERT_EQ(curr, ((char *) key.data()) + key.size());
}

TEST(triplestore, key_to_sp) {
    Blob sub(SUBJECT);
    Blob pred(PREDICATE);
    const std::size_t expected_len = sub.pack_size(false) +
                                     pred.pack_size(false) +
                                     3 * sizeof(uint8_t);

    Blob key;
    ASSERT_EQ(sp_to_key(sub, pred, &key), HXHIM_SUCCESS);
    EXPECT_EQ(key.size(), expected_len);

    // copy
    {
        Blob subject;
        Blob predicate;

        EXPECT_EQ(key_to_sp(key, subject, predicate, true), HXHIM_SUCCESS);

        EXPECT_NE(subject.data(), SUBJECT.data());
        EXPECT_EQ(subject.size(), SUBJECT.size());
        EXPECT_EQ(subject.data_type(), hxhim_data_t::HXHIM_DATA_BYTE);
        EXPECT_EQ(memcmp(subject.data(), SUBJECT.data(), subject.size()), 0);

        EXPECT_NE(predicate.data(), PREDICATE.data());
        EXPECT_EQ(predicate.size(), PREDICATE.size());
        EXPECT_EQ(predicate.data_type(), hxhim_data_t::HXHIM_DATA_BYTE);
        EXPECT_EQ(memcmp(predicate.data(), PREDICATE.data(), predicate.size()), 0);
    }

    // reference
    {
        Blob subject;
        Blob predicate;

        EXPECT_EQ(key_to_sp(key, subject, predicate, false), HXHIM_SUCCESS);

        EXPECT_EQ(subject.data(), key.data());
        EXPECT_EQ(subject.size(), SUBJECT.size());
        EXPECT_EQ(subject.data_type(), hxhim_data_t::HXHIM_DATA_BYTE);
        EXPECT_EQ(memcmp(subject.data(), SUBJECT.data(), subject.size()), 0);

        EXPECT_EQ(predicate.data(), ((char *) key.data()) + subject.size());
        EXPECT_EQ(predicate.size(), PREDICATE.size());
        EXPECT_EQ(predicate.data_type(), hxhim_data_t::HXHIM_DATA_BYTE);
        EXPECT_EQ(memcmp(predicate.data(), PREDICATE.data(), predicate.size()), 0);
    }
}
