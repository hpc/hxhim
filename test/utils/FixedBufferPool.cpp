#include <cstring>
#include <sstream>

#include <gtest/gtest.h>

#include "FixedBufferPool.hpp"

TEST(FixedBufferPool, usage) {
    const std::size_t TEST_ALLOC_SIZE = 8;
    const std::size_t TEST_REGIONS = 16;
    typedef FixedBufferPool<TEST_ALLOC_SIZE, TEST_REGIONS> TEST_FBP;

    void **alloc = new void *[TEST_REGIONS]();

    // release more memory locations each time
    for(std::size_t release = 0; release < TEST_REGIONS; release++) {
        // acquire memory
        for(int i = 0; i < TEST_REGIONS; i++) {
            alloc[i] = TEST_FBP::Instance().acquire(TEST_ALLOC_SIZE);
        }

        // all used
        EXPECT_EQ(TEST_FBP::Instance().used(), TEST_REGIONS);
        EXPECT_EQ(TEST_FBP::Instance().unused(), 0);

        // release some regions
        for(int i = 0; i < release; i++) {
            TEST_FBP::Instance().release(alloc[i]);
        }

        // check for leaks (some)
        EXPECT_EQ(TEST_FBP::Instance().used(), TEST_REGIONS - release);
        EXPECT_EQ(TEST_FBP::Instance().unused(), release);

        // cleanup
        for(int i = release; i < TEST_REGIONS; i++) {
            TEST_FBP::Instance().release(alloc[i]);
        }

        // check for leaks (none)
        EXPECT_EQ(TEST_FBP::Instance().used(), 0);
        EXPECT_EQ(TEST_FBP::Instance().unused(), TEST_REGIONS);
    }

    delete [] alloc;
}

TEST(FixedBufferPool, dump) {
    const std::string src = "ABCDEFGHIJKLMNOP";

    // manually generate dumped version of src
    std::stringstream expected;
    for(char const c : src) {
        expected << std::setw(2) << std::setfill('0') << (uint16_t) (uint8_t) c << " ";
    }
    const std::string expected_str = expected.str();

    const std::size_t TEST_ALLOC_SIZE = 4;
    const std::size_t TEST_REGIONS = 4;
    typedef FixedBufferPool<TEST_ALLOC_SIZE, TEST_REGIONS> TEST_FBP;

    char **ptrs = new char *[TEST_REGIONS]();
    for(std::size_t i = 0; i < TEST_REGIONS; i++) {
        // place src into the memory region
        ptrs[i] = TEST_FBP::Instance().acquire<char>(4);
        memcpy(ptrs[i], src.c_str() + (i * TEST_ALLOC_SIZE), TEST_ALLOC_SIZE);

        // check region dump against expected dump
        std::stringstream region_dump;
        TEST_FBP::Instance().dump(i, region_dump);
        EXPECT_EQ(region_dump.str(), expected_str.substr(i * 3 * TEST_ALLOC_SIZE, 3 * TEST_ALLOC_SIZE));
    }

    // check the entire pool dump
    std::stringstream pool_dump;
    TEST_FBP::Instance().dump(pool_dump);
    EXPECT_EQ(pool_dump.str(), expected_str);

    // cleanup
    for(std::size_t i = 0; i < TEST_REGIONS; i++) {
        TEST_FBP::Instance().release(ptrs[i]);
    }
    delete [] ptrs;
}

TEST(FixedBufferPool, too_large_request) {
    typedef int Test_t;
    const std::size_t TEST_ALLOC_SIZE = sizeof(Test_t);
    const std::size_t TEST_REGIONS = 4;
    typedef FixedBufferPool<TEST_ALLOC_SIZE, TEST_REGIONS> TEST_FBP;

    // acquire void *
    EXPECT_EQ(TEST_FBP::Instance().acquire(TEST_ALLOC_SIZE + 1), nullptr);

    // acquire Test_t *
    EXPECT_EQ(TEST_FBP::Instance().acquire<Test_t>(2), nullptr);

    // neither acquires should have used space
    EXPECT_EQ(TEST_FBP::Instance().used(), 0);
}
