#ifndef FIXED_BUFFER_POOL_MEMORY_MANAGER
#define FIXED_BUFFER_POOL_MEMORY_MANAGER

#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <type_traits>

/**
 * FixedBufferPool
 * This class distributes memory addresses from a fixed size pool of memory.
 *     - If zero bytes are requested, acquire will return nullptr
 *     - If too many bytes are requested, acquire will return nullptr
 *     - If there are no available regions, acquire will block.
*/
class FixedBufferPool {
    public:
        FixedBufferPool(const std::size_t alloc_size, const std::size_t regions);
        ~FixedBufferPool();

        /* @description Returns the address of a unused fixed size memory region        */
        template <typename T, typename = std::enable_if_t<!std::is_same<T, void>::value> >
        T *acquire(const std::size_t count = 1);
        void *acquire(const std::size_t size);

        /* @description Releases the given address back into the pool                   */
        template <typename T, typename = std::enable_if_t<!std::is_same<T, void>::value> >
        void release(T *ptr);
        void release(void *ptr);

        /* @description Utility function to get the total size of the memory pool       */
        std::size_t size() const;

        /* @description Utility function to get the size of each region                 */
        std::size_t alloc_size() const;

        /* @description Utility function to get the total number of regions             */
        std::size_t regions() const;

        /* @description Utility function to get number of unused memory regions         */
        std::size_t unused() const;

        /* @description Utility function to get number of used memory regions           */
        std::size_t used() const;

        /* @description Utility function to get starting address of memory pool         */
        const void *const pool() const;

        /* @description Utility function to dump the contents of a region               */
        std::ostream &dump(const std::size_t region, std::ostream &stream = std::cout) const;

        /* @description Utility function to dump the contents of the entire memory pool */
        std::ostream &dump(std::ostream &stream = std::cout) const;

    private:
        FixedBufferPool(const FixedBufferPool& copy)            = delete;
        FixedBufferPool(const FixedBufferPool&& copy)           = delete;

        FixedBufferPool& operator=(const FixedBufferPool& rhs)  = delete;
        FixedBufferPool& operator=(const FixedBufferPool&& rhs) = delete;

        /* @description Private utility function to dump the contents of a region          */
        std::ostream &dump_region(const std::size_t region, std::ostream &stream = std::cout) const;

        /* @description A fixed count of how much memory is in a region of memory          */
        const std::size_t alloc_size_;

        /* @description A fixed count of how many memory regions are available in the pool */
        const std::size_t regions_;

        /* @description The totoal amount of memory available in the memory pool           */
        const std::size_t pool_size_;

        /* Memory pool where pointers returned from acquire will come from                 */
        void *pool_;

        /* Concurrency Variables                                                           */
        mutable std::mutex mutex_;
        mutable std::condition_variable cv_;

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
            void *addr;
            Node *next;

            Node(void *ptr = nullptr, Node* node = nullptr)
                : addr(ptr), next(node)
            {}
        };

        /* @description An array of nodes that are allocated in the constructor            */
        Node *nodes_;

        /* @description A list of unused nodes. The head is popped off when acquiring an
         * address. A released address will use nodes_ to find the address of it's record
         * and use that address to push_front on unused_.
         */
        Node *unused_;

        /* @description a counter for keeping track of the number of regions used (instead
         * of iterating through the entire list of unused_ to figure out how many nodes
         * are unused)
         */
        std::size_t used_;
};

/**
 * acquire
 * Acquires a memory region from the pool for use.
 *   - If zero bytes are request, nullptr will be returned.
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
template <typename T, typename IsNotVoid>
T *FixedBufferPool::acquire(const std::size_t count) {
    void *addr = acquire(sizeof(T) * count);
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
template <typename T, typename IsNotVoid>
void FixedBufferPool::release(T *ptr) {
    if (ptr) {
        // release underlying memory first
        ptr->~T();

        // release ptr
        release((void *)ptr);
    }
}

#endif
