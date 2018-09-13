#include "utils/FixedBufferPool.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/** @description wrapper for FixedBufferPool mlog statements */
#define FBP_LOG(level, fmt, ...) mlog(level, "%s (%zu bytes x %zu/%zu regions free): " fmt, name_.c_str(), alloc_size_, regions_ - used_, regions_, ##__VA_ARGS__)

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
    pool_(nullptr),
    mutex_(),
    cv_(),
    nodes_(nullptr),
    unused_(nullptr),
    used_(0),
    requested(0),
    count(0)
{
    std::unique_lock<std::mutex> lock(mutex_);
    FBP_LOG(FBP_INFO, "Attemptiny to initialize");

    if (!regions_) {
        FBP_LOG(FBP_CRIT, "There must be at least 1 region of size %zu", alloc_size_);
        throw std::runtime_error("There must be at least 1 region of size " + std::to_string(alloc_size_));
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
 * FixedBufferPool Destructor
 */
FixedBufferPool::~FixedBufferPool() {
    std::unique_lock<std::mutex> lock(mutex_);

    for(std::size_t i = 1; i < regions_; i++) {
        nodes_[i - i].addr = nullptr;
        nodes_[i - 1].next = &nodes_[i];
    }
    nodes_[regions_ - 1].addr = nullptr;
    nodes_[regions_ - 1].next = nullptr;

    delete [] nodes_;
    nodes_ = nullptr;

    unused_ = nullptr;

    ::operator delete(pool_);
    pool_ = nullptr;

    FBP_LOG(FBP_INFO, "Average Allocation Size: %f (%zu/%zu)", ((double) requested) / count, requested, count);

    FBP_LOG(FBP_INFO, "Destructed");
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
 * @param size the total number of bytes requested
 * @return A pointer to a memory region of size pool_size_
 */
void *FixedBufferPool::acquire(const std::size_t size) {
    // 0 bytes
    if (!size) {
        FBP_LOG(FBP_DBG, "Got request for a size 0 buffer");
        return nullptr;
    }

    // too big
    if (size > alloc_size_) {
        FBP_LOG(FBP_WARN, "Requested allocation size too big: %zu bytes", size);
        return nullptr;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    // wait until a slot opens up
    while (!unused_) {
        FBP_LOG(FBP_DBG, "Waiting for a size %zu buffer", size);
        cv_.wait(lock, [&]{ return unused_; });
    }

    FBP_LOG(FBP_DBG, "A size %zu buffer is available", size);

    // set the return address to the head of the unused list
    // and clear the memory region
    void *ret = unused_->addr;

    // move list of unused regions_ to the next region
    Node *next = unused_->next;
    unused_->next = nullptr;
    unused_ = next;

    used_++;

    const std::size_t remaining = regions_ - used_;
    FBP_LOG(FBP_DBG, "Acquired a size %zu buffer (%p)", size, ret);

    if (!remaining) {
        FBP_LOG(FBP_WARN, "0 regions left.");
    }

    requested += size;
    count++;

    return ret;
}

/**
 * release
 * Releases the memory region pointed to back into the pool. If
 * the pointer does not belong to the pool, nothing will happen.
 *
 * @param ptr A void * acquired through FixedBufferPool::acquire
 */
void FixedBufferPool::release(void *ptr) {
    if (!ptr) {
        FBP_LOG(FBP_DBG1, "Attempted to free a nullptr");
        return;
    }

    std::unique_lock<std::mutex> lock(mutex_);

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

    // add the address to the front of available spots
    Node *front = &nodes_[index];
    front->next = unused_;
    unused_ = front;

    used_--;

    cv_.notify_all();

    FBP_LOG(FBP_DBG, "Freed %p", ptr);
}

/**
 * release_array
 * Releases the array to back into the pool without destructing the elements
 *
 * @tparam ptr   T * acquired through FixedBufferPool::acquire
 * @param  count number of elements (not used)
 */
void FixedBufferPool::release_array(void *ptr, const std::size_t count) {
    if (ptr) {
        release((void *) ptr);
    }
}

/**
 * size
 *
 * @return the total size of the pool of memory
 */
std::size_t FixedBufferPool::size() const {
    return pool_size_;
}

/**
 * alloc_size
 *
 * @return the size of each allocated region
 */
std::size_t FixedBufferPool::alloc_size() const {
    return alloc_size_;
}

/**
 * regions
 *
 * @return the total number of memory regions
 */
std::size_t FixedBufferPool::regions() const {
    return regions_;
}

/**
 * name
 *
 * @return the name
 */
std::string FixedBufferPool::name() const {
    return name_;
}

/**
 * unused
 *
 * @return number of memory regions_ that are not currently being used
 */
std::size_t FixedBufferPool::unused() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return regions_ - used_;
}

/**
 * used
 *
 * @return number of memory regions_ that are currently being used
 */
std::size_t FixedBufferPool::used() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return used_;
}

/**
 * pool
 *
 * @return starting address of the memory pool
 */
const void *FixedBufferPool::pool() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return pool_;
}

/**
 * dump
 *
 * @param region which region to view
 * @param stream stream to place human readable version of region into
 * @return a reference to the input stream
*/
std::ostream &FixedBufferPool::dump(const std::size_t region, std::ostream &stream) const {
    if (region >= regions_) {
        return stream;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    return dump_region(region, stream);
}

/**
 * dump
 *
 * @param stream stream to place human readable version of the memory pool into
 * @return a reference to the input stream
*/
std::ostream &FixedBufferPool::dump(std::ostream &stream) const {
    std::unique_lock<std::mutex> lock(mutex_);

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
std::ostream &FixedBufferPool::dump_region(const std::size_t region, std::ostream &stream) const {
    // region check is done in the public function
    // locking is done in the public functions

    std::ios::fmtflags orig(stream.flags());
    for(std::size_t i = 0; i < alloc_size_; i++) {
        stream << std::setw(2) << std::setfill('0') << (uint16_t) (uint8_t) ((char *)nodes_[region].addr)[i] << " ";
    }

    stream.flags(orig);
    return stream;
}
