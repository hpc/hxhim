#include <gtest/gtest.h>

#include "utils/MemoryManager.hpp"

TEST(MemoryManager, different_alloc_size) {
    FixedBufferPool *fbp1 = MemoryManager::FBP(128, 256);
    EXPECT_NE(fbp1, nullptr);

    FixedBufferPool *fbp2 = MemoryManager::FBP(129, 256);
    EXPECT_NE(fbp2, nullptr);

    EXPECT_NE(fbp1, fbp2);
}

TEST(MemoryManager, different_regions) {
    FixedBufferPool *fbp1 = MemoryManager::FBP(128, 256);
    EXPECT_NE(fbp1, nullptr);

    FixedBufferPool *fbp2 = MemoryManager::FBP(128, 257);
    EXPECT_NE(fbp2, nullptr);

    EXPECT_NE(fbp1, fbp2);
}

TEST(MemoryManager, duplicate) {
    FixedBufferPool *fbp1 = MemoryManager::FBP(128, 256);
    EXPECT_NE(fbp1, nullptr);

    FixedBufferPool *fbp2 = MemoryManager::FBP(128, 256);
    EXPECT_NE(fbp2, nullptr);

    EXPECT_EQ(fbp1, fbp2);
}

TEST(MemoryManager, zero) {
    EXPECT_EQ(MemoryManager::FBP(0, 128), nullptr);
    EXPECT_EQ(MemoryManager::FBP(128, 0), nullptr);
}
