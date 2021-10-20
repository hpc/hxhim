#ifndef TRANSPORT_BPUT_MESSAGE_HPP
#define TRANSPORT_BPUT_MESSAGE_HPP

#include "transport/Messages/SubjectPredicate.hpp"

namespace Transport {

namespace Request {

struct BPut final : SubjectPredicate {
    BPut(const std::size_t max = 0);
    ~BPut();

    void alloc(const std::size_t max);
    std::size_t add(Blob subject, Blob predicate, Blob object);
    int steal(BPut *from, const std::size_t i);
    int cleanup();

    Blob *objects;
};

}

namespace Response {

struct BPut final : SubjectPredicate {
    BPut(const std::size_t max = 0);
    ~BPut();

    void alloc(const std::size_t max);
    std::size_t add(Blob subject, Blob predicate, int status);
    int steal(BPut *from, const std::size_t i);
    int cleanup();
};

}

}

#endif
