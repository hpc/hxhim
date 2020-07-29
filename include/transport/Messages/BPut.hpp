#ifndef TRANSPORT_BPUT_MESSAGE_HPP
#define TRANSPORT_BPUT_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "utils/Blob.hpp"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct BPut final : Request {
    BPut(const std::size_t max = 0);
    ~BPut();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BPut *from, const std::size_t i);
    int cleanup();

    Blob *subjects;
    Blob *predicates;
    hxhim_object_type_t *object_types;
    Blob *objects;

    struct {
        void **subjects;
        void **predicates;
    } orig;
};

}

namespace Response {

struct BPut final : Response {
    BPut(const std::size_t max = 0);
    ~BPut();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BPut *from, const std::size_t i);
    int cleanup();

    struct {
        Blob *subjects;
        Blob *predicates;
    } orig;

    BPut *next;
};

}

}

#endif
