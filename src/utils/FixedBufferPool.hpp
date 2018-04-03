#ifndef FIXED_BUFFER_POOL_MEMORY_MANAGER
#define FIXED_BUFFER_POOL_MEMORY_MANAGER

#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <type_traits>

/**
 * FixedBufferPool
 * This class distributes memory addresses from a fixed size pool of memory.
 *
 * @tparam alloc_size_  fixed size of each region given to the user.
 * @tparam regions_     how many slots of size alloc_size_ there are for use
*/
template <const std::size_t alloc_size_,
          const std::size_t regions_,
          typename = std::enable_if_t<alloc_size_ &&  // alloc_size_ must be at least 1 byte
                                      regions_> >     // there must be at least 1 region that can be used
class FixedBufferPool {
    public:
        /* @description Gets an instance of FixedBufferPool for use                   */
        static FixedBufferPool &Instance();

        /* @description Locates and returns the address of a fixed size memory region */
        template <typename T, typename = std::enable_if_t<!std::is_same<T, void>::value> >
        T *acquire(const std::size_t count = 1);
        void *acquire(const std::size_t size = alloc_size_);

        /* @description Releases the given address back into the pool                 */
        template <typename T>
        void release(T *ptr);
        void release(void *ptr);

        /* @description Utility function to get the total size of the memory pool     */
        std::size_t size() const;

        /* @description Utility function to get the size of each region               */
        std::size_t alloc_size() const;

        /* @description Utility function to get the total number of regions           */
        std::size_t regions() const;

        /* @description Utility function to get number of unused memory regions       */
        std::size_t unused() const;

        /* @description Utility function to get number of used memory regions         */
        std::size_t used() const;

        /* @description Utility function to get starting address of memory pool       */
        const void * const pool() const;

    private:
        FixedBufferPool();
        FixedBufferPool(const FixedBufferPool& copy)            = delete;
        FixedBufferPool(const FixedBufferPool&& copy)           = delete;
        ~FixedBufferPool();

        FixedBufferPool& operator=(const FixedBufferPool& rhs)  = delete;
        FixedBufferPool& operator=(const FixedBufferPool&& rhs) = delete;

        /* Memory pool where pointers returned from acquire will come from            */
        void *pool_;

        /* Concurrency Variables                                                      */
        mutable std::mutex mutex_;
        mutable std::condition_variable cv_;

         /*
         * Node
         *
         * This structure is stored in both an array and a linked list.
         * The array is used to keep memory usage constant across runtime.
         * The list is used to keep track of which memory regiions are
         * available for acquiring.
         */
        struct Node {
            void *addr;
            Node *next;
            bool used;

            Node(void *ptr = nullptr, Node* node = nullptr)
                : addr(ptr), next(node), used(false)
            {}
        };

        /* @description A fixed count of how many memory regions are available in the pool */
        const std::size_t pool_size_;

        /* @description An array of nodes that are allocated in the constructor            */
        Node *nodes_;

        /* @description A list of unused nodes. The head is popped off when acquiring an
         * address. A released address will use nodes_ to find the address of it's record
         * and use that address to push_front on unused_.
         */
        Node *unused_;
};

#include "FixedBufferPool.cpp"

#endif
