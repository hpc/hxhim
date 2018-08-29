#include "utils/MemoryManager.hpp"

FixedBufferPool *MemoryManager::FBP(const std::size_t alloc_size, const std::size_t regions, const std::string &name) {
    if (!alloc_size || !regions) {
        return nullptr;
    }

    static MemoryManager mm;

    Pools::iterator it = mm.pools.find(name);
    if (it == mm.pools.end()) {
        std::pair<Pools::iterator, bool> insert = mm.pools.insert(std::make_pair(name, new FixedBufferPool(alloc_size, regions, name)));
        it = insert.first;
    }

    return it->second;
}

MemoryManager::MemoryManager()
    : pools()
{}

MemoryManager::~MemoryManager() {
    for(Pools::value_type const &fbp : pools) {
        delete fbp.second;
    }
}
