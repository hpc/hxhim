#ifndef TRANSPORT_BPUT_MESSAGE_HPP
#define TRANSPORT_BPUT_MESSAGE_HPP

#include "transport/Messages/SubjectPredicate.hpp"

namespace Transport {

namespace Request {

struct BPut final : SubjectPredicate {
    BPut(const std::size_t max = 0);
    ~BPut();

    std::size_t size() const;

    void alloc(const std::size_t max);
    int steal(BPut *from, const std::size_t i);
    int cleanup();

    Blob *objects;
};

}

namespace Response {

struct BPut final : SubjectPredicate {
    BPut(const std::size_t max = 0);
    ~BPut();

    std::size_t size() const;

    void alloc(const std::size_t max);
    int steal(BPut *from, const std::size_t i);
    int cleanup();
};

}

}

#endif
