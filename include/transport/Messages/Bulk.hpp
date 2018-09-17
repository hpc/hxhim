#ifndef TRANSPORT_BULK_MESSAGE_HPP
#define TRANSPORT_BULK_MESSAGE_HPP

#include <cstddef>

#include "utils/FixedBufferPool.hpp"

namespace Transport {

struct Bulk {
    public:
        Bulk();
        virtual ~Bulk();

        virtual int alloc(const std::size_t max) = 0;
        virtual int cleanup() = 0;

        int *ds_offsets;
        std::size_t count;

    protected:
        int alloc(const std::size_t max, FixedBufferPool *fbp);
        int cleanup(FixedBufferPool *fbp);
};

}

#endif
