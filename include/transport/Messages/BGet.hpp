#ifndef TRANSPORT_BGET_MESSAGE_HPP
#define TRANSPORT_BGET_MESSAGE_HPP

#include "transport/Messages/SubjectPredicate.hpp"

namespace Transport {

namespace Request {

struct BGet final : SubjectPredicate {
    BGet(const std::size_t max = 0);
    ~BGet();

    std::size_t size() const;

    void alloc(const std::size_t max);
    int steal(BGet *from, const std::size_t i);
    int cleanup();

    hxhim_data_t *object_types;
};

}

namespace Response {

struct BGet final : SubjectPredicate {
    BGet(const std::size_t max = 0);
    ~BGet();

    std::size_t size() const;

    void alloc(const std::size_t max);
    int steal(BGet *bget, const std::size_t i);
    int cleanup();

    Blob *objects;
};

}

}

#endif
