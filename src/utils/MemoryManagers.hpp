#ifndef HXHIM_MEMORY_ALLOCATORS
#define HXHIM_MEMORY_ALLOCATORS

#include "FixedBufferPool.hpp"

namespace Memory {
    typedef FixedBufferPool<512, 1024> FBP_MEDIUM;

    // TODO: Add more memory pools
    // TODO: Add a function to select which memory pool to use
}

#endif
