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
    cv_(),
    addrs_(),
    used_(0),
    stats()
{
    std::unique_lock<std::mutex> lock(mutex_);
    FBP_LOG(FBP_DBG, "Attempting to initialize");

    if (!alloc_size_) {
        FBP_LOG(FBP_CRIT, "Each allocation should be at least 1 byte");
        throw std::runtime_error(name_ + ": Each allocation should be at least 1 byte");
    }

    if (!regions_) {
        FBP_LOG(FBP_CRIT, "There must be at least 1 region of size %zu", alloc_size_);
        throw std::runtime_error(name_ + ": There must be at least 1 region of size " + std::to_string(alloc_size_));
    }

    FBP_LOG(FBP_INFO, "Created");
}

/**
 * FixedBufferPool Destructor
 */
FixedBufferPool::~FixedBufferPool() {
    std::unique_lock<std::mutex> lock(mutex_);

    if (used_) {
        FBP_LOG(FBP_CRIT, "Destructing with %zu memory regions still in use", used_);
        for(decltype(addrs_)::value_type const &addr : addrs_) {
            FBP_LOG(FBP_DBG, "    Address %p (%zu bytes) still allocated", addr.first, addr.second);
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
        FBP_LOG(FBP_WARN, "Got request for a size 0 buffer");
        return nullptr;
    }

    // too big
    if (size > alloc_size_) {
        FBP_LOG(FBP_ERR, "Requested allocation size too big: %zu bytes", size);
        return nullptr;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    // wait until a slot opens up
    bool waited = false;
    while (!(regions_ - used_)) {
        waited = true;
        FBP_LOG(FBP_WARN, "Waiting for a size %zu buffer", size);
        cv_.wait(lock, [&]{ return regions_ - used_; });
    }

    if (waited) {
        FBP_LOG(FBP_WARN, "A size %zu buffer is available", size);
    }

    // use alloc_size_ instead of size in order to replicate what FixedBufferPool does
    void *ret = ::operator new(alloc_size_);
    memset(ret, 0, alloc_size_);

    // store the allocation in a map
    addrs_[ret] = size;
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
void FixedBufferPool::releaseImpl(void *ptr, const std::size_t size) {
    if (!ptr) {
        FBP_LOG(FBP_DBG1, "Attempted to free a nullptr");
        return;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    decltype(addrs_)::const_iterator it = addrs_.find(ptr);
    if (it == addrs_.end()) {
        FBP_LOG(FBP_ERR, "Attempted to free address (%p) that was not allocated by %s", ptr, name_.c_str());
    }
    else {
        if (it->second != size) {
            FBP_LOG(FBP_DBG, "Release size of %p (%zu) does not match allocation size (%zu)", ptr, size, it->second);
        }

        addrs_.erase(it);
        ::operator delete(ptr);
        used_--;
        cv_.notify_all();

        FBP_LOG(FBP_DBG, "Freed %zu bytes at %p", it->second, ptr);
    }
}

#endif
