#ifndef HXHIM_MEMORY_ALLOCATORS
#define HXHIM_MEMORY_ALLOCATORS

#include "FixedBufferPool.hpp"

namespace Memory {
    typedef FixedBufferPool<128, 256> MESSAGE_BUFFER;

    // TODO: Add more memory pools
    // TODO: Add a function to select which memory pool to use
}

#endif
