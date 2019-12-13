#ifndef TRANSPORT_SP_HPP
#define TRANSPORT_SP_HPP

#include "transport/messages/Blob.hpp"

namespace Transport {

template <typename Blob_t>
struct SP {
    Blob_t subject;
    Blob_t predicate;

    std::size_t size() const {
        return subject.total_size() +
            predicate.total_size();
    }

    int pack(void *&buf, std::size_t &bufsize) const {
        return ((subject.serialize(buf, bufsize)   == TRANSPORT_SUCCESS) &&
                (predicate.serialize(buf, bufsize) == TRANSPORT_SUCCESS))?TRANSPORT_SUCCESS:TRANSPORT_ERROR;
    }

    int unpack(void *&buf, std::size_t &bufsize) {
        return ((subject.deserialize(buf, bufsize)   == TRANSPORT_SUCCESS) &&
                (predicate.deserialize(buf, bufsize) == TRANSPORT_SUCCESS))?TRANSPORT_SUCCESS:TRANSPORT_ERROR;
    }
};

}

#endif
