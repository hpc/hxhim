#ifndef FIXED_BUFFER_POOL_HPP
#define FIXED_BUFFER_POOL_HPP

#include <condition_variable>
#include <cstddef>
#include <cstring>
#include <mutex>
#include <ostream>
#include <type_traits>
#include <utility>

#ifndef DEBUG

/** @description wrapper for FixedBufferPool mlog statements */
#define FBP_LOG(level, fmt, ...) mlog(level, "%s (%zu bytes x %zu/%zu regions free): " fmt, name_.c_str(), alloc_size_, regions_ - used_, regions_, ##__VA_ARGS__)

#else
#include <set>

/** @description wrapper for Debug FixedBufferPool mlog statements */
#define FBP_LOG(level, fmt, ...) mlog(level, "Debug %s (%zu bytes x %zu/%zu regions free): " fmt, name_.c_str(), alloc_size_, regions_ - used_, regions_, ##__VA_ARGS__)

#endif

/**
 * FixedBufferPool
 * This class distributes memory addresses from a fixed size pool of memory.
 *     - If zero bytes are requested, acquire will return nullptr
 *     - If too many bytes are requested, acquire will return nullptr
 *     - If there are no available regions, acquire will block.
*/
class FixedBufferPool {
    public:
        FixedBufferPool(const std::size_t alloc_size, const std::size_t regions, const std::string &name = "FixedBufferPool");
        ~FixedBufferPool();

        /* @description Returns the address of a unused fixed size memory region           */
        template <typename T, typename... Args, typename = std::enable_if_t<!std::is_same<T, void>::value> >
        T *acquire(Args&&... args);
        void *acquire(const std::size_t size);

        template <typename T, typename = std::enable_if_t<!std::is_same<T, void>::value> >
        T *acquire_array(const std::size_t count = 1);

        /* @description Releases the given address back into the pool                      */
        template <typename T, typename = std::enable_if_t<!std::is_same<T, void>::value> >
        void release(T *ptr);
        void release(void *ptr, const std::size_t size);

        template <typename T, typename = std::enable_if_t<!std::is_same<T, void>::value> >
        void release_array(T *ptr, const std::size_t count);

        /* @description Utility function to get the total size of the memory pool          */
        std::size_t size() const;

        /* @description Utility function to get the size of each region                    */
        std::size_t alloc_size() const;

        /* @description Utility function to get the total number of regions                */
        std::size_t regions() const;

        /* @description Utility function to get the name of this FixedBufferPool           */
        std::string name() const;

        /* @description Utility function to get number of unused memory regions            */
        std::size_t unused() const;

        /* @description Utility function to get number of used memory regions              */
        std::size_t used() const;

        #ifndef DEBUG
        /* @description Utility function to get starting address of memory pool            */
        const void *pool() const;

        /* @description Utility function to dump the contents of a region                  */
        std::ostream &dump(const std::size_t region, std::ostream &stream) const;

        /* @description Utility function to dump the contents of the entire memory pool    */
        std::ostream &dump(std::ostream &stream) const;
        #endif

    private:
        FixedBufferPool(const FixedBufferPool& copy)            = delete;
        FixedBufferPool(const FixedBufferPool&& copy)           = delete;

        FixedBufferPool& operator=(const FixedBufferPool& rhs)  = delete;
        FixedBufferPool& operator=(const FixedBufferPool&& rhs) = delete;

        #ifndef DEBUG
        /* @description Private utility function to dump the contents of a region          */
        std::ostream &dump_region(const std::size_t region, std::ostream &stream) const;

        #endif

        /** @description The actual acquire function */
        void *acquireImpl(const std::size_t size);

        /** @description The actual release function */
        void releaseImpl(void *ptr, const std::size_t rec_size);

        const std::string name_;

        /* @description A fixed count of how much memory is in a region of memory          */
        const std::size_t alloc_size_;

        /* @description A fixed count of how many memory regions are available in the pool */
        const std::size_t regions_;

        /* @description The totoal amount of memory available in the memory pool           */
        const std::size_t pool_size_;

        /* Concurrency Variables                                                           */
        mutable std::mutex mutex_;

        #ifndef DEBUG
        mutable std::condition_variable cv_;

        /* Memory pool where pointers returned from acquire will come from                 */
        void *pool_;

        /*
         * Node
         *
         * This structure is stored in both an array and a linked list.
         * The array is used to keep memory usage constant across runtime.
         * The list is used to keep track of which memory regions are
         * available for acquiring.
         *
         * Acquiring a region simply pops a Node off of the unused_ list
         * (this Node is still in the array).
         *
         * Releasing a region is simply pushing this Node to the front
         * of the unused_ list.
         *
         */
        struct Node {
            enum Type {
                UNUSED,
                SINGLE,
                ARRAY,
            };

            std::size_t size;
            void *addr;
            Node *next;

            Node(const std::size_t size = UNUSED, void *ptr = nullptr, Node* node = nullptr)
                : size(size), addr(ptr), next(node)
            {}
        };

        /* @description An array of nodes that are allocated in the constructor            */
        Node *nodes_;

        /* @description A list of unused nodes. The head is popped off when acquiring an
         * address. A released address will use nodes_ to find the address of it's record
         * and use that address to push_front on unused_.
         */
        Node *unused_;

        #else
        std::set <void *> addrs_;
        #endif

        /* @description a counter for keeping track of the number of regions used (instead
         * of iterating through the entire list of unused_ to figure out how many nodes
         * are unused)
         */
        std::size_t used_;

        /** @description statistics */
        struct {
            std::size_t max_size;
            std::size_t max_used;
        } stats;
};

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
template <typename T, typename... Args, typename>
T *FixedBufferPool::acquire(Args&&... args) {
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
template <typename T, typename>
T *FixedBufferPool::acquire_array(const std::size_t count) {
    void *addr = acquireImpl(sizeof(T) * count);
    if (addr) {
        return new ((T *) addr) T();
    }

    return nullptr;
}

/**
 * release
 * Releases the memory region pointed to back into the pool. If
 * the pointer does not belong to the pool, nothing will happen.
 *
 * @tparam ptr T * acquired through FixedBufferPool::acquire
 */
template <typename T, typename>
void FixedBufferPool::release(T *ptr) {
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
 * @tparam ptr    T * acquired through FixedBufferPool::acquire
 * @param  count  number of elements to destroy
 */
template <typename T, typename>
void FixedBufferPool::release_array(T *ptr, const std::size_t count) {
    if (ptr) {
        for(std::size_t i = 0; i < count; i++) {
            ptr[i].~T();
        }

        releaseImpl((void *) ptr, sizeof(T) * count);
    }
}

#endif
