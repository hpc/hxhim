#ifndef TRANSPORT_BDELETE_MESSAGE_HPP
#define TRANSPORT_BDELETE_MESSAGE_HPP

#include "transport/Messages/SubjectPredicate.hpp"

namespace Transport {

namespace Request {

struct BDelete final : SubjectPredicate {
    BDelete(const std::size_t max = 0);
    ~BDelete();

    void alloc(const std::size_t max);
    std::size_t add(Blob subject, Blob predicate);
    int steal(BDelete *from, const std::size_t i);
    int cleanup();
};

}

namespace Response {

struct BDelete final : SubjectPredicate {
    BDelete(const std::size_t max = 0);
    ~BDelete();

    void alloc(const std::size_t max);
    std::size_t add(Blob subject, Blob predicate, int status);
    int steal(BDelete *from, const std::size_t i);
    int cleanup();
};

}

}

#endif
