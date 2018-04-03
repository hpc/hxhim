#include <gtest/gtest.h>

#include "FixedBufferPool.hpp"

const std::size_t TEST_ALLOC_SIZE = 8;
const std::size_t TEST_REGIONS = 16;
typedef FixedBufferPool<TEST_ALLOC_SIZE, TEST_REGIONS> TEST_FBP;

TEST(FixedBufferPool, usage) {
    void **alloc = new void *[TEST_REGIONS]();

    // release more memory locations each time
    for(std::size_t release = 0; release < TEST_REGIONS; release++) {
        // acquire memory
        for(int i = 0; i < TEST_REGIONS; i++) {
            alloc[i] = TEST_FBP::Instance().acquire();
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
