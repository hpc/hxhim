#include <gtest/gtest.h>

#include "utils/MemoryManager.hpp"

TEST(MemoryManager, different_names) {
    FixedBufferPool *fbp1 = MemoryManager::FBP(128, 256, "FBP 1");
    EXPECT_NE(fbp1, nullptr);

    FixedBufferPool *fbp2 = MemoryManager::FBP(128, 256, "FBP 2");
    EXPECT_NE(fbp2, nullptr);

    EXPECT_NE(fbp1, fbp2);
}

TEST(MemoryManager, same_names) {
    FixedBufferPool *fbp1 = MemoryManager::FBP(128, 256, "FBP 1");
    EXPECT_NE(fbp1, nullptr);

    FixedBufferPool *fbp2 = MemoryManager::FBP(128, 256, "FBP 1");
    EXPECT_NE(fbp2, nullptr);

    EXPECT_EQ(fbp1, fbp2);
}

TEST(MemoryManager, zero) {
    EXPECT_EQ(MemoryManager::FBP(0, 128), nullptr);
    EXPECT_EQ(MemoryManager::FBP(128, 0), nullptr);
}
