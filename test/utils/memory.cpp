#include "gtest/gtest.h"

#include "utils/memory.hpp"

TEST(memory, alloc_dealloc) {
    void *ptr = alloc(1024);
    EXPECT_NE(ptr, nullptr);
    dealloc(ptr);
}

TEST(memory, construct_destruct) {
    struct Struct {
        Struct (const int val)
          : ptr(construct<int>(val))
        {}

        ~Struct() {
            destruct(ptr);
        }

        int *ptr;
    };

    const int val = 1234;
    struct Struct *ptr = construct<struct Struct>(val);
    ASSERT_NE(ptr, nullptr);
    ASSERT_NE(ptr->ptr, nullptr);
    EXPECT_EQ(*ptr->ptr, val);
    destruct(ptr);
}

TEST(memory, array) {
    const std::size_t count = 10;
    int *ptr = alloc_array<int>(count);
    ASSERT_NE(ptr, nullptr);
    dealloc_array(ptr, count);
}
