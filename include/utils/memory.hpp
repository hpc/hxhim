#ifndef HXHIM_MEMORY_HPP
#define HXHIM_MEMORY_HPP

#include <new>
#include <utility>

// Simple wrappers around ::new and ::delete to reduce typing.
// Allows for the actual allocation/deallocation functions to be
// replaced without having to modify every file that calls them.

// These calls to new/delete end up being replaced by jemalloc, if it is available

void *alloc(const std::size_t size);
void dealloc(void *ptr);

template <typename T, typename... Args>
T *construct(Args&&... args) {
    return new T(std::forward<Args>(args)...);
}

template <typename T>
void destruct(T *ptr) {
    delete ptr;
}

// wraps initialzing each element of an array
// basically std::vector<T>::vector(size, value) without having to involve std::move
template <typename T, typename... Args>
T *alloc_array(const std::size_t count, Args&&... args) {
    if (!count) {
        return nullptr;
    }

    T *array = static_cast<T*>(::operator new[](sizeof(T) * count));
    for(std::size_t i = 0; i < count; i++) {
        new (&(array[i])) T(std::forward<Args>(args)...);
    }

    return array;
}

template <typename T>
void dealloc_array(T *ptr, const std::size_t count = 0) {
    if (ptr) {
        for(std::size_t i = count; i; i--) {
            ptr[i - 1].~T();
        }
    }

    ::operator delete[](ptr);
}

#endif
