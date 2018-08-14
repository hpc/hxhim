#ifndef TRANSPORT_BULK_MESSAGE_HPP
#define TRANSPORT_BULK_MESSAGE_HPP

#include <cstddef>

namespace Transport {

struct Bulk {
    Bulk(const std::size_t max = 0);
    virtual ~Bulk();

    virtual int alloc(const std::size_t max);
    virtual int cleanup();

    int *ds_offsets;
    std::size_t count;
};

}

#endif
