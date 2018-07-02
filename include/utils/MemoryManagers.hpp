#ifndef MEMORY_ALLOCATORS
#define MEMORY_ALLOCATORS

#include <map>

#include "FixedBufferPool.hpp"

class Memory {
    public:
        static FixedBufferPool *Pool(const std::size_t alloc_size, const std::size_t regions);

    private:
        Memory();
        ~Memory();

        std::map<std::size_t, std::map<std::size_t, FixedBufferPool *> > pools;
};

#endif
