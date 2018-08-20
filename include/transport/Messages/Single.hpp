#ifndef TRANSPORT_SINGLE_MESSAGE_HPP
#define TRANSPORT_SINGLE_MESSAGE_HPP

namespace Transport {

struct Single {
    Single();
    virtual ~Single();

    int ds_offset;
};

}

#endif
