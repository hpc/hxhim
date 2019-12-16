#ifndef HXHIM_MEMORY_HPP
#define HXHIM_MEMORY_HPP

#include <cstdlib>
#include <new>
#include <utility>

#include "utils/enable_if_t.hpp"

void *alloc(const std::size_t size);
void dealloc(void *ptr);

template <typename T, typename... Args>
T *construct(Args&&... args) {
    T *ptr = static_cast<T *>(alloc(sizeof(T)));
    return new (ptr) T(std::forward<Args>(args)...);
}

template <typename T>
void destruct(T *ptr) {
    if (ptr) {
        ptr->~T();
    }
    dealloc(ptr);
}

template <typename T, typename... Args>
T *alloc_array(const std::size_t count, Args&&... args) {
    T *array = static_cast<T *>(alloc(sizeof(T) * count));
    for(std::size_t i = 0; i < count; i++) {
        new (&(array[i])) T(std::forward<Args>(args)...);
    }

    return array;
}

template <typename T>
void dealloc_array(T *ptr, const std::size_t count = 0) {
    if (ptr) {
        for(std::size_t i = 0; i < count; i++) {
            ptr[i].~T();
        }
    }
    dealloc(ptr);
}

#endif
