#ifndef TRANSPORT_BULK_MESSAGE_HPP
#define TRANSPORT_BULK_MESSAGE_HPP

#include <cstddef>

namespace Transport {

struct Bulk {
    public:
        Bulk();
        virtual ~Bulk();

        int *ds_offsets;
        std::size_t count;
        std::size_t max_count;

    protected:
        virtual int alloc(const std::size_t max);
        virtual int cleanup();
};

}

#endif
