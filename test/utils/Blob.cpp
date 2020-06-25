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

TEST(ReferenceBlob, has_data) {
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
        void *packed = alloc(len + sizeof(len));
        char *curr = (char *) packed;
        ASSERT_NE(refblob.pack(curr), nullptr);
        EXPECT_EQ(curr, ((char *) packed) + (len + sizeof(len)));

        // reference blob does not have a method to unpack

        dealloc(packed);
    }

    dealloc(ptr);
}

TEST(ReferenceBlob, reference) {
    ReferenceBlob src(&src, rand());

    const std::size_t buf_size = sizeof(src.data()) + sizeof(src.size());
    char *buf = new char[buf_size];
    char *curr = buf;

    EXPECT_EQ(src.pack_ref(curr), buf + buf_size);

    curr = buf;

    ReferenceBlob dst;
    EXPECT_EQ(dst.unpack_ref(curr), buf + buf_size);
    EXPECT_EQ(src.data(), dst.data());
    EXPECT_EQ(src.size(), dst.size());

    delete [] buf;
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

TEST(RealBlob, has_data) {
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
        void *packed = alloc(len + sizeof(len));
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

TEST(RealBlob, move) {
    void *ptr = alloc(len);
    memset(ptr, 0, len);

    RealBlob src(ptr, len);
    RealBlob move_constructor(std::move(src));

    EXPECT_EQ(src.data(), nullptr);
    EXPECT_EQ(src.size(), (std::size_t) 0);
    EXPECT_EQ(move_constructor.data(), ptr);
    EXPECT_EQ(move_constructor.size(), len);

    RealBlob move_assignment;
    EXPECT_EQ(move_assignment.data(), nullptr);
    EXPECT_EQ(move_assignment.size(), (std::size_t) 0);
    move_assignment = std::move(move_constructor);
    EXPECT_EQ(move_assignment.data(), ptr);
    EXPECT_EQ(move_assignment.size(), len);
}
