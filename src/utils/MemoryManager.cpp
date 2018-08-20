#include "utils/MemoryManager.hpp"

FixedBufferPool *MemoryManager::FBP(const std::size_t alloc_size, const std::size_t regions, const std::string &name) {
    if (!alloc_size || !regions) {
        return nullptr;
    }

    static MemoryManager mm;

    // find pools with the same allocation size
    Pools::iterator pools_it = mm.pools.find(alloc_size);
    if (pools_it == mm.pools.end()) {
        std::pair<std::map<std::size_t, std::map<std::size_t, FixedBufferPool *> >::iterator, bool> insert = mm.pools.insert(std::make_pair(alloc_size, std::map<std::size_t, FixedBufferPool *>()));
        pools_it = insert.first;
    }

    // find pool with same region count
    std::map<std::size_t, FixedBufferPool *>::iterator pool_it = pools_it->second.find(regions);
    if (pool_it == pools_it->second.end()) {
        std::pair<std::map<std::size_t, FixedBufferPool *>::iterator, bool> insert = pools_it->second.insert(std::make_pair(regions, new FixedBufferPool(alloc_size, regions, name)));
        pool_it = insert.first;
    }

    return pool_it->second;
}

MemoryManager::MemoryManager()
    : pools()
{}

MemoryManager::~MemoryManager() {
    for(Pools::value_type const &alloc_size : pools) {
        for(Pool::value_type const &region : alloc_size.second) {
            delete region.second;
        }
    }
}
