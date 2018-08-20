#ifndef MEMORY_MANAGER_HPP
#define MEMORY_MANAGER_HPP

#include <map>

#include "FixedBufferPool.hpp"

class MemoryManager {
    public:
        static FixedBufferPool *FBP(const std::size_t alloc_size, const std::size_t regions, const std::string &name = "FixedBufferPool");

    private:
        MemoryManager();
        ~MemoryManager();

        typedef std::map<std::size_t, FixedBufferPool *> Pool;
        typedef std::map<std::size_t, Pool> Pools;

        Pools pools;
};

#endif
