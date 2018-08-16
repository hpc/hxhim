#include "utils/MemoryManagers.hpp"

FixedBufferPool *Memory::FBP(const std::size_t alloc_size, const std::size_t regions, const std::string &name) {
    if (!alloc_size || !regions) {
        return nullptr;
    }

    static Memory manager;

    std::map<std::size_t, std::map<std::size_t, FixedBufferPool *> >::iterator alloc_it = manager.pools.find(alloc_size);
    if (alloc_it == manager.pools.end()) {
        std::pair<std::map<std::size_t, std::map<std::size_t, FixedBufferPool *> >::iterator, bool> insert = manager.pools.insert(std::make_pair(alloc_size, std::map<std::size_t, FixedBufferPool *>()));
        alloc_it = insert.first;
    }

    std::map<std::size_t, FixedBufferPool *>::iterator regions_it = alloc_it->second.find(regions);
    if (regions_it == alloc_it->second.end()) {
        std::pair<std::map<std::size_t, FixedBufferPool *>::iterator, bool> insert = alloc_it->second.insert(std::make_pair(regions, new FixedBufferPool(alloc_size, regions, name)));
        regions_it = insert.first;
    }

    return regions_it->second;
}

Memory::Memory()
  : pools()
{}

Memory::~Memory() {
    for(std::pair<const std::size_t, std::map<std::size_t, FixedBufferPool *> > const &alloc_size : pools) {
        for(std::pair<const std::size_t, FixedBufferPool *> const &regions : alloc_size.second) {
            delete regions.second;
        }
    }
}
