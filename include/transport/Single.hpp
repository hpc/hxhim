#ifndef TRANSPORT_SINGLE_MESSAGE_HPP
#define TRANSPORT_SINGLE_MESSAGE_HPP

#include <cstdint>

namespace Transport {

struct Single {
    Single();
    virtual ~Single();

    int ds_offset;
};

}

#endif
