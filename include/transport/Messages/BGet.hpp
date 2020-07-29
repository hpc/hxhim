#ifndef TRANSPORT_BGET_MESSAGE_HPP
#define TRANSPORT_BGET_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "utils/Blob.hpp"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct BGet final : Request {
    BGet(const std::size_t max = 0);
    ~BGet();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BGet *from, const std::size_t i);
    int cleanup();

    Blob *subjects;
    Blob *predicates;
    std::size_t *predicate_lens;
    hxhim_object_type_t *object_types;

    struct {
        // arrays of addresses from client
        // used by server when unpacking, and are sent back with response - otherwise not used
        // do not deallocate individual pointers
        void **subjects;
        void **predicates;
    } orig;

    void *ptr;
};

}

namespace Response {

struct BGet final : Response {
    BGet(const std::size_t max = 0);
    ~BGet();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BGet *bget, const std::size_t i);
    int cleanup();

    hxhim_object_type_t *object_types;
    Blob *objects;

    struct {
        // arrays of addresses from client
        // used by server when unpacking, and are sent back with response - otherwise not used
        // do not deallocate individual pointers
        Blob *subjects;
        Blob *predicates;
    } orig;

    BGet *next;
};

}

}

#endif
