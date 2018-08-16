#ifndef MEMORY_MANAGERS_HPP
#define MEMORY_MANAGERS_HPP

#include <map>

#include "FixedBufferPool.hpp"

class Memory {
    public:
        static FixedBufferPool *FBP(const std::size_t alloc_size, const std::size_t regions, const std::string &name = "FixedBufferPool");

    private:
        Memory();
        ~Memory();

        std::map<std::size_t, std::map<std::size_t, FixedBufferPool *> > pools;
};

#endif
