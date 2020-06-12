#include "utils/memory.hpp"

void *alloc(const std::size_t size) {
    return ::operator new(size);
}

void dealloc(void *ptr) {
    ::operator delete(ptr);
}
