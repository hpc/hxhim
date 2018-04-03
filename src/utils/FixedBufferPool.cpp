#include <iostream>
/**
 * Instance
 *
 * @return A reference to an instance of FixedBufferPool
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
FixedBufferPool<alloc_size_, regions_, Allowed> &FixedBufferPool<alloc_size_, regions_, Allowed>::Instance() {
    static FixedBufferPool fbp;
    return fbp;
}

/**
 * acquire
 * Acquires a memory region from the pool for use.
 * If there is no region available, the function blocks
 * until one is available. The default constructor of
 * type T is called once the memory location has been
 * acquired.
 *
 * Note that void * pointers should not be allocated
 * using the templated version of acquire.
 *
 * @return A pointer to a memory region of size pool_size_
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
template <typename T, typename IsNotVoid>
T *FixedBufferPool<alloc_size_, regions_, Allowed>::acquire(const std::size_t count) {
    return new ((T *) acquire(sizeof(T) * count)) T();
}

/**
 * acquire
 * Acquires a memory region from the pool for use.
 * If there is no region available, the function blocks
 * until one is available.
 *
 * @return A pointer to a memory region of size pool_size_
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
void *FixedBufferPool<alloc_size_, regions_, Allowed>::acquire(const std::size_t size) {
    // bad size
    if (!size || (size > alloc_size_)) {
        return nullptr;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    if (!pool_) {
        return nullptr;
    }

    // wait until a slot opens up
    while (!unused_) {
        cv_.wait(lock, [&]{ return unused_; });
    }

    // set the return address to the head of the unused list
    // and clear the memory region
    void *ret = unused_->addr;
    unused_->used = true;
    memset(ret, 0, alloc_size_);

    // move list of unused regions_ to the next region
    Node *next = unused_->next;
    unused_->next = nullptr;
    unused_ = next;

    return ret;
}

/**
 * release
 * Releases the memory region pointed to back into the pool. If
 * the pointer does not belong to the pool, nothing will happen.
 *
 * @tparam ptr T * acquired through FixedBufferPool::acquire
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
template <typename T>
void FixedBufferPool<alloc_size_, regions_, Allowed>::release(T *ptr) {
    if (ptr) {
        // release underlying memory first
        ptr->~T();

        // release ptr
        release((void *)ptr);
    }
}

/**
 * release
 * Releases the memory region pointed to back into the pool. If
 * the pointer does not belong to the pool, nothing will happen.
 *
 * @param ptr A void * acquired through FixedBufferPool::acquire
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
void FixedBufferPool<alloc_size_, regions_, Allowed>::release(void *ptr) {
    if (!ptr) {
        return;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    // if the pointer is not within the memory pool, do nothing
    if (((char *)ptr < (char *)pool_) ||
        (((char *)pool_ + pool_size_)) < (char *)ptr) {
        return;
    }

    // offset of the pointer from the front of the pool
    const std::size_t offset = (char *)ptr - (char *)pool_;

    // // if the pointer isn't a multiple of the of the allocation size, do nothing
    // if (offset % alloc_size_) {
    //     return;
    // }

    // index of this pointer in the fixed size array
    const std::size_t index = offset / alloc_size_;

    // check for double frees (to prevent loops)
    if (!nodes_[index].used) {
        std::cout << "double free" << std::endl;
        return;
    }

    // add the address to the front of available spots
    Node *front = &nodes_[index];
    front->next = unused_;
    front->used = false;
    unused_ = front;

    cv_.notify_all();
}

/**
 * size
 *
 * @return the total size of the pool of memory
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
std::size_t FixedBufferPool<alloc_size_, regions_, Allowed>::size() const {
    return pool_size_;
}

/**
 * alloc_size
 *
 * @return the size of each allocated region
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
std::size_t FixedBufferPool<alloc_size_, regions_, Allowed>::alloc_size() const {
    return alloc_size_;
}

/**
 * regions
 *
 * @return the total number of memory regions
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
std::size_t FixedBufferPool<alloc_size_, regions_, Allowed>::regions() const {
    return regions_;
}

/**
 * unused
 *
 * @return number of memory regions_ that are not currently being used
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
std::size_t FixedBufferPool<alloc_size_, regions_, Allowed>::unused() const {
    std::unique_lock<std::mutex> lock(mutex_);

    // get number of unused slots
    std::size_t count = 0;
    Node *u = unused_;
    while (u) {
        u = u->next;
        count++;
    }

    return count;
}

/**
 * used
 *
 * @return number of memory regions_ that are currently being used
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
std::size_t FixedBufferPool<alloc_size_, regions_, Allowed>::used() const {
    return pool_?regions_ - unused():0;
}

/**
 * pool
 *
 * @return starting address of the memory pool
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
const void * const FixedBufferPool<alloc_size_, regions_, Allowed>::pool() const {
    return pool_;
}

/**
 * Default constructor of FixedBufferPool
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
FixedBufferPool<alloc_size_, regions_, Allowed>::FixedBufferPool()
    : pool_(nullptr),
      mutex_(),
      cv_(),
      pool_size_(alloc_size_ * regions_),
      nodes_(nullptr),
      unused_(nullptr)
{
    std::unique_lock<std::mutex> lock(mutex_);

    // allocate memory at runtime
    pool_ = ::operator new(pool_size_);

    try {
        // allocate the array of nodes
        nodes_ = new Node[regions_]();
    }
    catch (...) {
        ::operator delete(pool_);
        throw;
    }

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
}

/**
 * FixedBufferPool Destructor
 */
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename Allowed>
FixedBufferPool<alloc_size_, regions_, Allowed>::~FixedBufferPool() {
    std::unique_lock<std::mutex> lock(mutex_);

    // reset
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
}
