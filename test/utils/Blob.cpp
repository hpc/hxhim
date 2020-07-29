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
        EXPECT_EQ(nullblob.data(), nullptr);
        EXPECT_EQ(nullblob.size(), (std::size_t) 0);

        char *ptr = nullptr;
        EXPECT_EQ(nullblob.pack(ptr), nullptr);

        ptr = (char *) (uintptr_t) rand();
        EXPECT_EQ(nullblob.pack(ptr), nullptr);
    }

    // nullptr, non-zero len
    {
        ReferenceBlob nullblob_with_len(nullptr, len);
        EXPECT_EQ(nullblob_with_len.data(), nullptr);
        EXPECT_EQ(nullblob_with_len.size(), len);

        char *ptr = nullptr;
        EXPECT_EQ(nullblob_with_len.pack(ptr), nullptr);

        ptr = (char *) (uintptr_t) rand();
        EXPECT_EQ(nullblob_with_len.pack(ptr), nullptr);
    }
}

TEST(ReferenceBlob, pack_unpack) {
    void *ptr = alloc(len);
    memset(ptr, 0, len);

    ReferenceBlob refblob(ptr, len);
    EXPECT_EQ(refblob.data(), ptr);
    EXPECT_EQ(refblob.size(), len);

    // pack into a nullptr
    {
        char *null = nullptr;
        EXPECT_EQ(refblob.pack(null), nullptr);
    }

    // pack the reference blob
    {
        void *packed = alloc(refblob.pack_size());
        char *curr = (char *) packed;
        ASSERT_NE(refblob.pack(curr), nullptr);
        EXPECT_EQ(curr, ((char *) packed) + refblob.pack_size());

        // reference blob does not have a method to unpack the actual data

        dealloc(packed);
    }

    // pack the reference instead of the data being pointed to
    {
        void *packed = alloc(refblob.pack_ref_size());
        char *curr = (char *) packed;
        ASSERT_NE(refblob.pack_ref(curr), nullptr);
        EXPECT_EQ(curr, ((char *) packed) + refblob.pack_ref_size());

        // unpack the packed data
        curr = (char *) packed;
        ReferenceBlob unpacked;
        EXPECT_NE(unpacked.unpack_ref(curr), nullptr);
        EXPECT_EQ(unpacked.data(), refblob.data());
        EXPECT_EQ(unpacked.size(), refblob.size());

        dealloc(packed);
    }

    dealloc(ptr);
}

TEST(ReferenceBlob, assignment) {
    void *ptr = alloc(len);

    // assignment
    {
        ReferenceBlob rhs(ptr, len);
        ReferenceBlob lhs(nullptr, 0);

        lhs = rhs;

        // pointer and length overwritten with rhs values
        EXPECT_EQ(lhs.data(), ptr);
        EXPECT_EQ(lhs.size(), len);

        // rhs did not change
        EXPECT_EQ(rhs.data(), ptr);
        EXPECT_EQ(rhs.size(), len);
    }

    // move assignment
    {
        ReferenceBlob rhs(ptr, len);
        ReferenceBlob lhs(nullptr, 0);

        lhs = std::move(rhs);

        // pointer and length overwritten with rhs values
        EXPECT_EQ(lhs.data(), ptr);
        EXPECT_EQ(lhs.size(), len);

        // rhs did not change
        EXPECT_EQ(rhs.data(), ptr);
        EXPECT_EQ(rhs.size(), len);
    }

    dealloc(ptr);
}

TEST(RealBlob, null) {
    // nullptr, zero len
    {
        RealBlob nullblob(nullptr, 0);
        EXPECT_EQ(nullblob.data(), nullptr);
        EXPECT_EQ(nullblob.size(), (std::size_t) 0);

        char *ptr = nullptr;
        EXPECT_EQ(nullblob.pack(ptr), nullptr);

        ptr = (char *) (uintptr_t) rand();
        EXPECT_EQ(nullblob.pack(ptr), nullptr);
    }

    // nullptr, non-zero len
    {
        RealBlob nullblob_with_len(nullptr, len);
        EXPECT_EQ(nullblob_with_len.data(), nullptr);
        EXPECT_EQ(nullblob_with_len.size(), len);

        char *ptr = nullptr;
        EXPECT_EQ(nullblob_with_len.pack(ptr), nullptr);

        ptr = (char *) (uintptr_t) rand();
        EXPECT_EQ(nullblob_with_len.pack(ptr), nullptr);
    }
}

TEST(RealBlob, pack_unpack) {
    void *ptr = alloc(len);
    memset(ptr, 0, len);

    RealBlob realblob(ptr, len);
    EXPECT_EQ(realblob.data(), ptr);
    EXPECT_EQ(realblob.size(), len);

    // pack into a nullptr
    {
        char *null = nullptr;
        EXPECT_EQ(realblob.pack(null), nullptr);
    }

    // pack the real blob
    {
        void *packed = alloc(realblob.pack_size());
        char *curr = (char *) packed;
        ASSERT_NE(realblob.pack(curr), nullptr);
        EXPECT_EQ(curr, ((char *) packed) + (len + sizeof(len)));

        // unpack with unknown length constructor
        {
            char *curr = (char *) packed;
            RealBlob unknown_len(curr);
            EXPECT_EQ(curr, ((char *) packed) + len + sizeof(len));                        // pointer shifted
            EXPECT_EQ(unknown_len.size(), realblob.size());                                // length matches
            EXPECT_NE(unknown_len.data(), realblob.data());                                // not a reference
            EXPECT_EQ(memcmp(realblob.data(), unknown_len.data(), unknown_len.size()), 0); // data matches
        }

        dealloc(packed);
    }

    // length and data are known, but data needs to be copied
    {
        char *curr = (char *) ptr;
        RealBlob copy(len, curr);
        EXPECT_EQ(curr, ((char *) ptr));                       // pointer did not shift
        EXPECT_EQ(copy.size(), len);                           // length matches
        EXPECT_NE(copy.data(), ptr);                           // not a reference
        EXPECT_EQ(memcmp(ptr, copy.data(), copy.size()), 0);   // data matches
    }

    // do not deallocate ptr, since realblob took ownership
}

TEST(RealBlob, assignment) {
    // assignment
    {
        void *ptr = alloc(len);

        RealBlob rhs(ptr, len);
        RealBlob lhs(nullptr, 0);

        lhs = rhs;

        // pointer and length overwritten with rhs values
        EXPECT_EQ(lhs.data(), ptr);
        EXPECT_EQ(lhs.size(), len);

        // rhs zeroed
        EXPECT_EQ(rhs.data(), nullptr);
        EXPECT_EQ(rhs.size(), 0);
    }

    // move assignment
    {
        void *ptr = alloc(len);

        RealBlob rhs(ptr, len);
        RealBlob lhs(nullptr, 0);

        lhs = std::move(rhs);

        // pointer and length overwritten with rhs values
        EXPECT_EQ(lhs.data(), ptr);
        EXPECT_EQ(lhs.size(), len);

        // rhs zeroed
        EXPECT_EQ(rhs.data(), nullptr);
        EXPECT_EQ(rhs.size(), 0);
    }
}
