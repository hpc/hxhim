#ifndef HXHIM_TRANSPORT_BGETOP_HPP
#define HXHIM_TRANSPORT_BGETOP_HPP

#include <stdexcept>

#include "hxhim/constants.h"
#include "transport/messages/Message.hpp"

namespace Transport {

template <typename Blob_t>
struct GetOp {
    Blob_t subject;
    Blob_t predicate;
    std::size_t num_recs;
    hxhim_get_op_t op;

    std::size_t size() const {
        return subject.total_size() +
            predicate.total_size() +
            sizeof(num_recs) + sizeof(op);
    }

    int pack(void *&buf, std::size_t &bufsize) const {
        if ((subject.serialize(buf, bufsize)   != HXHIM_SUCCESS) ||
            (predicate.serialize(buf, bufsize) != HXHIM_SUCCESS)) {
            return HXHIM_ERROR;
        }

        memset(buf, &num_recs, sizeof(num_recs));
        (char *&) buf += sizeof(num_recs);
        bufsize -= sizeof(num_recs);

        memset(buf, &op, sizeof(op));
        (char *&) buf += sizeof(op);
        bufsize -= sizeof(op);

        return HXHIM_SUCCESS;
    }

    int unpack(void *&buf, std::size_t &bufsize) {
        if ((subject.deserialize(buf, bufsize)   != HXHIM_SUCCESS) ||
            (predicate.deserialize(buf, bufsize) != HXHIM_SUCCESS)) {
            return HXHIM_ERROR;
        }

        memset(&num_recs, buf, sizeof(num_recs));
        (char *&) buf += sizeof(num_recs);
        bufsize -= sizeof(num_recs);

        memset(&op, buf, sizeof(op));
        (char *&) buf += sizeof(op);
        bufsize -= sizeof(op);

        return HXHIM_SUCCESS;
    }
};

namespace Request {

template <typename Blob_t>
class BGetOp : public virtual Message <Transport::GetOp <Blob_t> > {
    public:
        BGetOp(const std::size_t max_count);
        BGetOp(void *buf, std::size_t bufsize);
};

}

namespace Response {

template <typename Blob_t>
class BGetOp : public virtual Message <Transport::GetOp <Blob_t> >{
    public:
        BGetOp(const std::size_t max_count);
        BGetOp(void *buf, std::size_t bufsize);
};

}

}

#include "transport/messages/BGetOp.tpp"

#endif
