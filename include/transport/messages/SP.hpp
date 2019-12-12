#ifndef HXHIM_TRANSPORT_SP_HPP
#define HXHIM_TRANSPORT_SP_HPP

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
        return ((subject.serialize(buf, bufsize)   == HXHIM_SUCCESS) &&
                (predicate.serialize(buf, bufsize) == HXHIM_SUCCESS))?HXHIM_SUCCESS:HXHIM_ERROR;
    }

    int unpack(void *&buf, std::size_t &bufsize) {
        return ((subject.deserialize(buf, bufsize)   == HXHIM_SUCCESS) &&
                (predicate.deserialize(buf, bufsize) == HXHIM_SUCCESS))?HXHIM_SUCCESS:HXHIM_ERROR;
    }
};

}

#endif
