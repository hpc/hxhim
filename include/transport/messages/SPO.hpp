#ifndef HXHIM_TRANSPORT_SPO_HPP
#define HXHIM_TRANSPORT_SPO_HPP

#include "transport/messages/Blob.hpp"

namespace Transport {

template <typename Blob_t>
struct SPO {
    Blob_t subject;
    Blob_t predicate;
    Blob_t object;

    std::size_t size() const {
        return subject.total_size() +
            predicate.total_size() +
            object.total_size();
    }

    int pack(void *&buf, std::size_t &bufsize) const {
        return ((subject.serialize(buf, bufsize)   == HXHIM_SUCCESS) &&
                (predicate.serialize(buf, bufsize) == HXHIM_SUCCESS) &&
                (object.serialize(buf, bufsize)    == HXHIM_SUCCESS))?HXHIM_SUCCESS:HXHIM_ERROR;
    }

    int unpack(void *&buf, std::size_t &bufsize) {
        return ((subject.deserialize(buf, bufsize)   == HXHIM_SUCCESS) &&
                (predicate.deserialize(buf, bufsize) == HXHIM_SUCCESS) &&
                (object.deserialize(buf, bufsize)    == HXHIM_SUCCESS))?HXHIM_SUCCESS:HXHIM_ERROR;
    }
};

};

#endif
