#ifndef HXHIM_CHECK_MEMORY
#define HXHIM_CHECK_MEMORY

#include <gtest/gtest.h>

#include "hxhim/private.hpp"

/**
 * CHECK_MEMORY
 * Checks the number of memory regions still allocated
 * right before HXHIM is closed.
 *
 * - The arrays FixedBufferPool should have 1 region left
 *   due to the HXHIN initialization creating an array.
 *
 * - The results FixedBufferPool should have 1 region left
 *   due to the results in the asynchronous PUT thread.
 *
 * @param hx A pointer to the HXHIM instance
 */
#define CHECK_MEMORY(hx) do {                                     \
        EXPECT_EQ((hx)->p->memory_pools.keys->used(),        0);  \
        EXPECT_EQ((hx)->p->memory_pools.buffers->used(),     0);  \
        EXPECT_EQ((hx)->p->memory_pools.ops_cache->used(),   0);  \
        EXPECT_EQ((hx)->p->memory_pools.arrays->used(),      1);  \
        EXPECT_EQ((hx)->p->memory_pools.requests->used(),    0);  \
        EXPECT_EQ((hx)->p->memory_pools.responses->used(),   0);  \
        EXPECT_EQ((hx)->p->memory_pools.result->used(),      0);  \
        EXPECT_EQ((hx)->p->memory_pools.results->used(),     1);  \
    } while (0)

#endif
