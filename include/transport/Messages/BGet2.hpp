#ifndef TRANSPORT_BGET2_MESSAGE_HPP
#define TRANSPORT_BGET2_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
#include "utils/Blob.hpp"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/constants.hpp"

namespace Transport {

namespace Request {

struct BGet2 final : Request {
    BGet2(const std::size_t max = 0);
    ~BGet2();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BGet2 *from, const std::size_t i);
    int cleanup();

    // used by src (client)
    Blob **subjects;
    Blob **predicates;
    std::size_t *predicate_lens;
    hxhim_type_t *object_types;

    // used by dst (server)
    Blob **objects;

    struct {
        // arrays of references - do not deallocate individual pointers
        void **subjects;
        void **predicates;
        void **objects;
        std::size_t **object_lens;
    } orig;
};

}

namespace Response {

struct BGet2 final : Response {
    BGet2(const std::size_t max = 0);
    ~BGet2();

    std::size_t size() const;

    int alloc(const std::size_t max);
    int steal(BGet2 *bget, const int ds);
    int cleanup();

    // used by dst (client)
    Blob **subjects;
    Blob **predicates;
    hxhim_type_t *object_types;

    // filled by dst (server)
    Blob **objects;

    struct {
        // arrays of references - do not deallocate individual pointers
        void **subjects;
        void **predicates;
        void **objects;
        std::size_t **object_lens;
    } orig;

    BGet2 *next;
};

}

}

#endif
