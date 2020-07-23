#include "gtest/gtest.h"

#include "utils/memory.hpp"

static const int VALUE = rand();

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

    struct Struct *ptr = construct<struct Struct>(VALUE);
    ASSERT_NE(ptr, nullptr);
    ASSERT_NE(ptr->ptr, nullptr);
    EXPECT_EQ(*ptr->ptr, VALUE);
    destruct(ptr);
}

TEST(memory, array) {
    const std::size_t count = 10;

    // built in type
    {
        int *ptr = alloc_array<int>(count, VALUE);
        ASSERT_NE(ptr, nullptr);
        for(std::size_t i = 0; i < count; i++) {
            EXPECT_EQ(ptr[i], VALUE);
        }
        dealloc_array(ptr, count);
    }

    // pointer
    {
        void **ptr = alloc_array<void *>(count, nullptr);
        ASSERT_NE(ptr, nullptr);
        for(std::size_t i = 0; i < count; i++) {
            EXPECT_EQ(ptr[i], nullptr);
        }
        dealloc_array(ptr, count);
    }

    // type with default constructor
    {
        struct WithDefaultConstructor {
            WithDefaultConstructor()
                : n(VALUE)
            {}

            const int n;
        };

        WithDefaultConstructor *ptr = alloc_array<WithDefaultConstructor>(count);
        ASSERT_NE(ptr, nullptr);
        for(std::size_t i = 0; i < count; i++) {
            EXPECT_EQ(ptr[i].n, VALUE);
        }
        dealloc_array(ptr, count);
    }

    // type without default constructor
    {
        struct WithoutDefaultConstructor {
            WithoutDefaultConstructor(const int value)
                : n(value)
            {}

            const int n;
        };

        WithoutDefaultConstructor *ptr = alloc_array<WithoutDefaultConstructor>(count, VALUE);
        ASSERT_NE(ptr, nullptr);
        for(std::size_t i = 0; i < count; i++) {
            EXPECT_EQ(ptr[i].n, VALUE);
        }
        dealloc_array(ptr, count);
    }
}
