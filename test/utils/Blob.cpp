#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <utility>

#include "utils/Blob.hpp"
#include "utils/memory.hpp"

const std::size_t len = 1024;

TEST(ReferenceBlob, null) {
    // nullptr, zero len
    {
        ReferenceBlob nullblob(nullptr, 0);
        EXPECT_EQ(nullblob.ptr, nullptr);
        EXPECT_EQ(nullblob.len, 0);

        char *ptr = nullptr;
        EXPECT_EQ(nullblob.pack(ptr), nullptr);

        ptr = (char *) (uintptr_t) rand();
        EXPECT_EQ(nullblob.pack(ptr), nullptr);
    }

    // nullptr, non-zero len
    {
        ReferenceBlob nullblob_with_len(nullptr, len);
        EXPECT_EQ(nullblob_with_len.ptr, nullptr);
        EXPECT_EQ(nullblob_with_len.len, len);

        char *ptr = nullptr;
        EXPECT_EQ(nullblob_with_len.pack(ptr), nullptr);

        ptr = (char *) (uintptr_t) rand();
        EXPECT_EQ(nullblob_with_len.pack(ptr), nullptr);
    }
}

TEST(ReferenceBlob, has_data) {
    void *ptr = alloc(len);
    memset(ptr, 0, len);

    ReferenceBlob refblob(ptr, len);
    EXPECT_EQ(refblob.ptr, ptr);
    EXPECT_EQ(refblob.len, len);

    // pack into a nullptr
    {
        char *null = nullptr;
        EXPECT_EQ(refblob.pack(null), nullptr);
    }

    // pack the reference blob
    {
        void *packed = alloc(len + sizeof(len));
        char *curr = (char *) packed;
        ASSERT_NE(refblob.pack(curr), nullptr);
        EXPECT_EQ(curr, ((char *) packed) + (len + sizeof(len)));

        // reference blob does not have a method to unpack

        dealloc(packed);
    }

    dealloc(ptr);
}

TEST(RealBlob, null) {
    // nullptr, zero len
    {
        RealBlob nullblob(nullptr, 0);
        EXPECT_EQ(nullblob.ptr, nullptr);
        EXPECT_EQ(nullblob.len, 0);

        char *ptr = nullptr;
        EXPECT_EQ(nullblob.pack(ptr), nullptr);

        ptr = (char *) (uintptr_t) rand();
        EXPECT_EQ(nullblob.pack(ptr), nullptr);
    }

    // nullptr, non-zero len
    {
        RealBlob nullblob_with_len(nullptr, len);
        EXPECT_EQ(nullblob_with_len.ptr, nullptr);
        EXPECT_EQ(nullblob_with_len.len, len);

        char *ptr = nullptr;
        EXPECT_EQ(nullblob_with_len.pack(ptr), nullptr);

        ptr = (char *) (uintptr_t) rand();
        EXPECT_EQ(nullblob_with_len.pack(ptr), nullptr);
    }
}

TEST(RealBlob, has_data) {
    void *ptr = alloc(len);
    memset(ptr, 0, len);

    RealBlob realblob(ptr, len);
    EXPECT_EQ(realblob.ptr, ptr);
    EXPECT_EQ(realblob.len, len);

    // pack into a nullptr
    {
        char *null = nullptr;
        EXPECT_EQ(realblob.pack(null), nullptr);
    }

    // pack the real blob
    {
        void *packed = alloc(len + sizeof(len));
        char *curr = (char *) packed;
        ASSERT_NE(realblob.pack(curr), nullptr);
        EXPECT_EQ(curr, ((char *) packed) + (len + sizeof(len)));

        // unpack with unknown length constructor
        {
            char *curr = (char *) packed;
            RealBlob unknown_len(curr);
            EXPECT_EQ(curr, ((char *) packed) + len + sizeof(len));                 // pointer shifted
            EXPECT_EQ(unknown_len.len, realblob.len);                               // length matches
            EXPECT_NE(unknown_len.ptr, realblob.ptr);                               // not a reference
            EXPECT_EQ(memcmp(realblob.ptr, unknown_len.ptr, unknown_len.len), 0);   // data matches
        }

        dealloc(packed);
    }

    // length and data are known, but data needs to be copied
    {
        char *curr = (char *) ptr;
        RealBlob copy(len, curr);
        EXPECT_EQ(curr, ((char *) ptr));                                            // pointer did not shift
        EXPECT_EQ(copy.len, len);                                                   // length matches
        EXPECT_NE(copy.ptr, ptr);                                                   // not a reference
        EXPECT_EQ(memcmp(ptr, copy.ptr, copy.len), 0);                              // data matches
    }

    // do not deallocate ptr, since realblob took ownership
}

TEST(RealBlob, move) {
    void *ptr = alloc(len);
    memset(ptr, 0, len);

    RealBlob src(ptr, len);
    RealBlob move_constructor(std::move(src));

    EXPECT_EQ(src.ptr, nullptr);
    EXPECT_EQ(src.len, 0);
    EXPECT_EQ(move_constructor.ptr, ptr);
    EXPECT_EQ(move_constructor.len, len);

    RealBlob move_assignment;
    EXPECT_EQ(move_assignment.ptr, nullptr);
    EXPECT_EQ(move_assignment.len, 0);
    move_assignment = std::move(move_constructor);
    EXPECT_EQ(move_assignment.ptr, ptr);
    EXPECT_EQ(move_assignment.len, len);
}
