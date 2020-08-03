#include "utils/memory.hpp"

void *alloc(const std::size_t size) {
    return size?::operator new(size):nullptr;
}

void dealloc(void *ptr) {
    ::operator delete(ptr);
}
