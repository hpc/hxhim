#ifndef TRANSPORT_BGET2_MESSAGE_HPP
#define TRANSPORT_BGET2_MESSAGE_HPP

#include <cstddef>

#include "hxhim/constants.h"
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
    int cleanup();

    // used by src
    void **subjects;
    std::size_t *subject_lens;
    void **predicates;
    std::size_t *predicate_lens;
    hxhim_type_t *object_types;

    // used by dst
    void **objects;
    std::size_t **object_lens;

    // filled in by src, to be sent back to src
    struct {
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

    void server_side_cleanup(void * args = nullptr);

    std::size_t size() const;

    int alloc(const std::size_t max);
    int merge(BGet2 *bget, const int ds);
    int cleanup();

    int *statuses;

    // used by dst
    void **subjects;
    std::size_t *subject_lens;
    void **predicates;
    std::size_t *predicate_lens;
    hxhim_type_t *object_types;

    // used by src
    void **objects;
    std::size_t **object_lens;

    // filled by src, to be sent to the dst
    struct {
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
