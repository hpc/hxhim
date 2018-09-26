#ifdef DEBUG

#include "utils/FixedBufferPool.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * FixedBufferPool Constructor
 *
 * @param alloc_size how much space a region contains
 * @param regions    how many regions there are
 * @param name       the identifier to use when printing log messages
 */
FixedBufferPool::FixedBufferPool(const std::size_t alloc_size, const std::size_t regions, const std::string &name)
  : name_(name),
    alloc_size_(alloc_size),
    regions_(regions),
    pool_size_(alloc_size_ * regions_),
    mutex_(),
    addrs_(),
    used_(0),
    stats()
{
    std::unique_lock<std::mutex> lock(mutex_);
    FBP_LOG(FBP_INFO, "Attempting to initialize");

    if (!regions_) {
        FBP_LOG(FBP_CRIT, "There must be at least 1 region of size %zu", alloc_size_);
        throw std::runtime_error("There must be at least 1 region of size " + std::to_string(alloc_size_));
    }

    FBP_LOG(FBP_DBG, "Created");
}

/**
 * FixedBufferPool Destructor
 */
FixedBufferPool::~FixedBufferPool() {
    std::unique_lock<std::mutex> lock(mutex_);

    if (used_) {
        FBP_LOG(FBP_INFO, "Destructing with %zu memory regions still in use", used_);
        for(void *addr : addrs_) {
            FBP_LOG(FBP_DBG, "    Address %p still allocated", addr);
            // do not delete pointers here to allow for valgrind to see leaks
        }
    }

    FBP_LOG(FBP_INFO, "Maximum granted allocation size: %zu", stats.max_size);
    FBP_LOG(FBP_INFO, "Maximum number of regions used at once: %zu", stats.max_used);
}

/**
 * acquire
 * Returns a pointer obtained with ::operator new
 *
 * @param size     The total number of bytes requested
 * @return A pointer
 */
void *FixedBufferPool::acquireImpl(const std::size_t size) {
    // 0 bytes
    if (!size) {
        FBP_LOG(FBP_DBG, "Got request for a size 0 buffer");
        return nullptr;
    }

    // too big
    if (size > alloc_size_) {
        FBP_LOG(FBP_ERR, "Requested allocation size too big: %zu bytes", size);
        return nullptr;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    // set the return address to the head of the unused list
    void *ret = ::operator new(size);
    addrs_.insert(ret);
    used_++;

    if (size > stats.max_size) {
        stats.max_size = size;
    }

    if (used_ > stats.max_used) {
        stats.max_used = used_;
    }

    FBP_LOG(FBP_DBG, "Acquired a size %zu buffer (%p)", size, ret);

    return ret;
}

/**
 * releaseImpl
 * Calls ::operator delete
 *
 * @param ptr   A void * acquired through FixedBufferPool::acquire
 */
void FixedBufferPool::releaseImpl(void *ptr, const std::size_t) {
    if (!ptr) {
        FBP_LOG(FBP_DBG1, "Attempted to free a nullptr");
        return;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    ::operator delete(ptr);
    addrs_.erase(ptr);
    used_--;

    FBP_LOG(FBP_DBG, "Freed %p", ptr);
}

#endif
