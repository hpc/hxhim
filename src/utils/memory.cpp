#include "utils/memory.hpp"

// explictly use malloc to allocate memory
// instead of relying on new to call malloc

void *alloc(const std::size_t size) {
    return malloc(size);
}

void dealloc(void *ptr) {
    free(ptr);
}
