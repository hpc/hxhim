#include <iomanip>

#include "utils/FixedBufferPool.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

template <typename Mutex_t, typename Cond_t>
bool FixedBufferPoolImpl <Mutex_t, Cond_t>::within(void * ptr) const {
    #ifndef DEBUG
    for(Node * n = nodes_; n; n = n->next) {
        if (n->addr == ptr) {
            return true;
        }
    }
    return false;
    #else
    return (addrs_.find(ptr) != addrs_.end());
#endif
}

#ifndef DEBUG

/**
 * FixedBufferPoolImpl Constructor
 *
 * @param alloc_size how much space a region contains
 * @param regions    how many regions there are
 * @param name       the identifier to use when printing log messages
 */
template <typename Mutex_t, typename Cond_t>
FixedBufferPoolImpl <Mutex_t, Cond_t>::FixedBufferPoolImpl(const std::size_t alloc_size, const std::size_t regions, const std::string &name)
  : name_(name),
    alloc_size_(alloc_size),
    regions_(regions),
    pool_size_(alloc_size_ * regions_),
    mutex_(),
    cv_(),
    pool_(nullptr),
    nodes_(nullptr),
    unused_(nullptr),
    used_(0),
    stats()
{
    std::unique_lock<Mutex_t> lock(mutex_);
    FBP_LOG(FBP_DBG, "Attempting to initialize");

    if (!alloc_size_) {
        FBP_LOG(FBP_CRIT, "Each allocation should be at least 1 byte");
        throw std::runtime_error(name_ + ": Each allocation should be at least 1 byte");
    }

    if (!regions_) {
        FBP_LOG(FBP_CRIT, "There must be at least 1 region of size %zu", alloc_size_);
        throw std::runtime_error(name_ + ": There must be at least 1 region of size " + std::to_string(alloc_size_));
    }

    try {
        pool_ = ::operator new(pool_size_);     // allocate memory at runtime
    }
    catch (...) {
        FBP_LOG(FBP_CRIT, "Failed to allocate main memory pool");
        throw;
    }

    try {
        // allocate the array of nodes
        nodes_ = new Node[regions_]();
    }
    catch (...) {
        FBP_LOG(FBP_CRIT, "Failed to allocate bookkeeping array");
        ::operator delete(pool_);
        throw;
    }

    // clear the pool
    memset(pool_, 0, pool_size_);

    // slot selection starts off in order
    std::size_t offset = 0;
    for(std::size_t i = 1; i < regions_; i++) {
        nodes_[i - 1].addr = (char *)pool_ + offset;
        nodes_[i - 1].next = &nodes_[i];

        offset += alloc_size_;
    }
    nodes_[regions_ - 1].addr = (char *)pool_ + offset;
    nodes_[regions_ - 1].next = nullptr;

    // first available region is the head of the list
    unused_ = nodes_;

    FBP_LOG(FBP_INFO, "Created");
}

/**
 * FixedBufferPoolImpl Destructor
 */
template <typename Mutex_t, typename Cond_t>
FixedBufferPoolImpl <Mutex_t, Cond_t>::~FixedBufferPoolImpl() {
    std::unique_lock<Mutex_t> lock(mutex_);

    if (used_) {
        FBP_LOG(FBP_INFO, "Destructing with %zu memory regions still in use", used_);
        for(std::size_t i = 0; i < regions_; i++) {
            if (nodes_[i].size) {
                FBP_LOG(FBP_DBG, "    Address %p (index %zu) still allocated with %zu bytes", nodes_[i].addr, i, nodes_[i].size);
            }
        }
    }

    FBP_LOG(FBP_INFO, "Maximum granted allocation size: %zu", stats.max_size);
    FBP_LOG(FBP_INFO, "Maximum number of regions used at once: %zu", stats.max_used);

    delete [] nodes_;
    nodes_ = nullptr;

    unused_ = nullptr;

    ::operator delete(pool_);
    pool_ = nullptr;
}

#endif

/**
 * acquire
 * Acquires a memory region from the pool for use.
 *   - If there is no region available, the function blocks
 *     until one is available.
 * The provided arguments will be used to call a constructor of
 * type T is called once the memory location has been acquired.
 *
 * Note that void * pointers should not be allocated
 * using the templated version of acquire.
 *
 * @param count the number of T objects that will be placed into the region
 * @return A pointer to a memory region of size pool_size_
 */
template <typename Mutex_t, typename Cond_t>
template <typename T, typename... Args, typename>
T *FixedBufferPoolImpl <Mutex_t, Cond_t>::acquire(Args&&... args) {
    void *addr = acquireImpl(sizeof(T));
    if (addr) {
        return new ((T *) addr) T(std::forward<Args>(args)...);
    }

    return nullptr;
}

/**
 * acquire_array
 * Acquires a memory region from the pool for use.
 *   - If zero bytes are requested, nullptr will be returned.
 *   - If the provided count results in too many bytes
 *     being requested, nullptr will be returned.
 *   - If there is no region available, the function blocks
 *     until one is available.
 * The default constructor of type T is called once
 * the memory location has been acquired.
 *
 * Note that void * pointers should not be allocated
 * using the templated version of acquire.
 *
 * @param count the number of T objects that will be placed into the region
 * @return A pointer to a memory region of size pool_size_
 */
template <typename Mutex_t, typename Cond_t>
template <typename T, typename>
T *FixedBufferPoolImpl <Mutex_t, Cond_t>::acquire_array(const std::size_t count) {
    void *addr = acquireImpl(sizeof(T) * count);
    if (addr) {
        return new ((T *) addr) T();
    }

    return nullptr;
}

/**
 * acquire
 * Acquires a memory region from the pool for use
 *
 * @param size the total number of bytes requested
 * @return A pointer to a memory region of size pool_size_
 */
template <typename Mutex_t, typename Cond_t>
void *FixedBufferPoolImpl <Mutex_t, Cond_t>::acquire(const std::size_t size) {
    return acquireImpl(size);
}

/**
 * release
 * Releases the memory region pointed to back into the pool. If
 * the pointer does not belong to the pool, nothing will happen.
 *
 * @tparam ptr T * acquired through FixedBufferPoolImpl::acquire
 */
template <typename Mutex_t, typename Cond_t>
template <typename T, typename>
void FixedBufferPoolImpl <Mutex_t, Cond_t>::release(T *ptr) {
    if (ptr) {
        // release underlying memory first
        ptr->~T();

        // release ptr
        releaseImpl((void *) ptr, sizeof(T));
    }
}

/**
 * release_array
 * Destructs each element and releases the array to back into the pool.
 *
 * @tparam ptr    T * acquired through FixedBufferPoolImpl::acquire
 * @param  count  number of elements to destroy
 */
template <typename Mutex_t, typename Cond_t>
template <typename T, typename>
void FixedBufferPoolImpl <Mutex_t, Cond_t>::release_array(T *ptr, const std::size_t count) {
    if (ptr) {
        for(std::size_t i = 0; i < count; i++) {
            ptr[i].~T();
        }

        releaseImpl((void *) ptr, sizeof(T) * count);
    }
}

/**
 * release
 * Releases the memory region pointed to back into the pool
 *
 * @param ptr A void * acquired through FixedBufferPoolImpl <Mutex_t, Cond_t>::acquire
 */
template <typename Mutex_t, typename Cond_t>
void FixedBufferPoolImpl <Mutex_t, Cond_t>::release(void *ptr, const std::size_t size) {
    releaseImpl(ptr, size);
}

/**
 * size
 *
 * @return the total size of the pool of memory
 */
template <typename Mutex_t, typename Cond_t>
std::size_t FixedBufferPoolImpl <Mutex_t, Cond_t>::size() const {
    return pool_size_;
}

/**
 * alloc_size
 *
 * @return the size of each allocated region
 */
template <typename Mutex_t, typename Cond_t>
std::size_t FixedBufferPoolImpl <Mutex_t, Cond_t>::alloc_size() const {
    return alloc_size_;
}

/**
 * regions
 *
 * @return the total number of memory regions
 */
template <typename Mutex_t, typename Cond_t>
std::size_t FixedBufferPoolImpl <Mutex_t, Cond_t>::regions() const {
    return regions_;
}

/**
 * name
 *
 * @return the name
 */
template <typename Mutex_t, typename Cond_t>
std::string FixedBufferPoolImpl <Mutex_t, Cond_t>::name() const {
    return name_;
}

/**
 * unused
 *
 * @return number of memory regions_ that are not currently being used
 */
template <typename Mutex_t, typename Cond_t>
std::size_t FixedBufferPoolImpl <Mutex_t, Cond_t>::unused() const {
    std::unique_lock<Mutex_t> lock(mutex_);
    return regions_ - used_;
}

/**
 * used
 *
 * @return number of memory regions_ that are currently being used
 */
template <typename Mutex_t, typename Cond_t>
std::size_t FixedBufferPoolImpl <Mutex_t, Cond_t>::used() const {
    std::unique_lock<Mutex_t> lock(mutex_);
    return used_;
}

#ifndef DEBUG
/**
 * pool
 *
 * @return starting address of the memory pool
 */
template <typename Mutex_t, typename Cond_t>
const void *FixedBufferPoolImpl <Mutex_t, Cond_t>::pool() const {
    std::unique_lock<Mutex_t> lock(mutex_);
    return pool_;
}

/**
 * dump
 *
 * @param region which region to view
 * @param stream stream to place human readable version of region into
 * @return a reference to the input stream
*/
template <typename Mutex_t, typename Cond_t>
std::ostream &FixedBufferPoolImpl <Mutex_t, Cond_t>::dump(const std::size_t region, std::ostream &stream) const {
    if (region >= regions_) {
        return stream;
    }

    std::unique_lock<Mutex_t> lock(mutex_);
    return dump_region(region, stream);
}

/**
 * dump
 *
 * @param stream stream to place human readable version of the memory pool into
 * @return a reference to the input stream
*/
template <typename Mutex_t, typename Cond_t>
std::ostream &FixedBufferPoolImpl <Mutex_t, Cond_t>::dump(std::ostream &stream) const {
    std::unique_lock<Mutex_t> lock(mutex_);

    for(std::size_t i = 0; i < regions_; i++) {
        dump_region(i, stream);
    }

    return stream;
}

/**
 * dump_region
 *
 * @param region which region to view
 * @param stream stream to place human readable version of region into
 * @return a reference to the input stream
*/
template <typename Mutex_t, typename Cond_t>
std::ostream &FixedBufferPoolImpl <Mutex_t, Cond_t>::dump_region(const std::size_t region, std::ostream &stream) const {
    // region check is done in the public function
    // locking is done in the public functions

    std::ios::fmtflags orig(stream.flags());
    for(std::size_t i = 0; i < alloc_size_; i++) {
        stream << std::setw(2) << std::setfill('0') << (uint16_t) (uint8_t) ((char *)nodes_[region].addr)[i] << " ";
    }

    stream.flags(orig);
    return stream;
}

/**
 * acquire
 * Acquires a memory region from the pool for use.
 *   - If zero bytes are request, nullptr will be returned.
 *   - If the too many bytes are requested, nullptr
 *     will be returned.
 *   - If there is no region available, the function blocks
 *     until one is available.
 * The default constructor of type T is called once
 * the memory location has been acquired.
 *
 * @param size     The total number of bytes requested
 * @return A pointer to a memory region of size pool_size_
 */
template <typename Mutex_t, typename Cond_t>
void *FixedBufferPoolImpl <Mutex_t, Cond_t>::acquireImpl(const std::size_t size) {
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

    std::unique_lock<Mutex_t> lock(mutex_);

    // wait until a slot opens up
    while (!unused_) {
        FBP_LOG(FBP_WARN, "Waiting for a size %zu buffer", size);
        cv_.wait(lock, [&]{ return unused_; });
    }

    FBP_LOG(FBP_DBG, "A size %zu buffer is available", size);

    // set the return address to the head of the unused list
    void *ret = unused_->addr;

    // store the allocation size
    unused_->size = size;

    // move list of unused regions_ to the next region
    Node *next = unused_->next;
    unused_->next = nullptr;
    unused_ = next;

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
 * Releases the memory region pointed to back into the pool. If
 * the pointer does not belong to the pool, nothing will happen.
 *
 * @param ptr   A void * acquired through FixedBufferPoolImpl <Mutex_t, Cond_t>::acquire
 * @param size  The size being deallocated
 */
template <typename Mutex_t, typename Cond_t>
void FixedBufferPoolImpl <Mutex_t, Cond_t>::releaseImpl(void *ptr, const std::size_t size) {
    if (!ptr) {
        FBP_LOG(FBP_DBG1, "Attempted to free a nullptr");
        return;
    }

    std::unique_lock<Mutex_t> lock(mutex_);

    // if the pointer is not within the memory pool, do nothing
    if (((char *)ptr < (char *)pool_) ||
        (((char *)pool_ + pool_size_)) < (char *)ptr) {
        FBP_LOG(FBP_ERR, "Attempted to free address (%p) that is not within the memory pool [%p, %p)", ptr, pool_, (char *)pool_ + pool_size_);
        return;
    }

    // offset of the pointer from the front of the pool
    const std::size_t offset = (char *)ptr - (char *)pool_;

    // if the pointer isn't a multiple of the of the allocation size, do nothing
    if (offset % alloc_size_) {
        FBP_LOG(FBP_WARN, "Address being freed (%p) is not aligned to an allocation region", ptr);
    }

    // index of this pointer in the fixed size array
    const std::size_t index = offset / alloc_size_;

    // check for double frees (to prevent loops)
    if (nodes_[index].next) {
        FBP_LOG(FBP_WARN, "Address being freed (%p) has already been freed", ptr);
        return;
    }

    // if the release size does not match the allocation size, warn
    if (nodes_[index].size != size) {
        FBP_LOG(FBP_DBG, "Release size of %p (%zu) does not match allocation size (%zu)", ptr, size, nodes_[index].size);
    }

    // reset size of allocation at that node
    const std::size_t actual_size = nodes_[index].size;
    nodes_[index].size = 0;

    // add the address to the front of available spots
    Node *front = &nodes_[index];
    front->next = unused_;
    unused_ = front;

    used_--;

    cv_.notify_all();

    FBP_LOG(FBP_DBG, "Freed %zu bytes at %p", actual_size, ptr);
}

#endif
