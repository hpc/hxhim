#ifndef MEMORY_MANAGER_HPP
#define MEMORY_MANAGER_HPP

#include <map>

#include "FixedBufferPool.hpp"

/**
 * MemoryManager
 * Returns a unique FixedBufferPool, given a name
 * The FixedBufferPools are indexed by name, not alloc_size/regions
 */
class MemoryManager {
    public:
        static FixedBufferPool *FBP(const std::size_t alloc_size, const std::size_t regions, const std::string &name = "FixedBufferPool");

    private:
        MemoryManager();
        ~MemoryManager();

        typedef std::map<std::string, FixedBufferPool *> Pools;
        Pools pools;
};

#endif
