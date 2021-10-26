#ifndef BPUT_MESSAGE_HPP
#define BPUT_MESSAGE_HPP

#include "message/SubjectPredicate.hpp"

namespace Message {

namespace Request {

struct BPut final : SubjectPredicate {
    BPut(const std::size_t max = 0);
    ~BPut();

    void alloc(const std::size_t max);
    std::size_t add(Blob subject, Blob predicate, Blob object);
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
