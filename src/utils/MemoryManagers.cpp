#include "MemoryManagers.hpp"

std::map<std::size_t, std::map<std::size_t, FixedBufferPool *> > Memory::pools;

FixedBufferPool *Memory::Pool(const std::size_t alloc_size, const std::size_t regions) {
    if (!alloc_size || !regions) {
        return nullptr;
    }

    std::map<std::size_t, std::map<std::size_t, FixedBufferPool *> >::iterator alloc_it = pools.find(alloc_size);
    if (alloc_it == pools.end()) {
        std::pair<std::map<std::size_t, std::map<std::size_t, FixedBufferPool *> >::iterator, bool> insert = pools.insert(std::make_pair(alloc_size, std::map<std::size_t, FixedBufferPool *>()));
        alloc_it = insert.first;
    }

    std::map<std::size_t, FixedBufferPool *>::iterator regions_it = alloc_it->second.find(regions);
    if (regions_it == alloc_it->second.end()) {
        std::pair<std::map<std::size_t, FixedBufferPool *>::iterator, bool> insert = alloc_it->second.insert(std::make_pair(regions, new FixedBufferPool(alloc_size, regions)));
        regions_it = insert.first;
    }

    return regions_it->second;
}

Memory::Memory() {}

Memory::~Memory() {
    for(std::pair<const std::size_t, std::map<std::size_t, FixedBufferPool *> > const &alloc_size : pools) {
        for(std::pair<const std::size_t, FixedBufferPool *> const &regions : alloc_size.second) {
            delete regions.second;
        }
    }
}
