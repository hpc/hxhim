#ifndef BGET_MESSAGE_HPP
#define BGET_MESSAGE_HPP

#include "message/SubjectPredicate.hpp"

namespace Message {

namespace Request {

struct BGet final : SubjectPredicate {
    BGet(const std::size_t max = 0);
    ~BGet();

    void alloc(const std::size_t max);
    std::size_t add(Blob subject, Blob predicate, hxhim_data_t object_type);
    int cleanup();

    hxhim_data_t *object_types;
};

}

namespace Response {

struct BGet final : SubjectPredicate {
    BGet(const std::size_t max = 0);
    ~BGet();

    void alloc(const std::size_t max);
    std::size_t add(Blob subject, Blob predicate, Blob &&object, int status);
    int steal(BGet *bget, const std::size_t i);
    int cleanup();

    Blob *objects;
};

}

}

#endif
