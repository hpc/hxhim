#ifndef TRANSPORT_BGETOP_HPP
#define TRANSPORT_BGETOP_HPP

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
        if ((subject.serialize(buf, bufsize)   != TRANSPORT_SUCCESS) ||
            (predicate.serialize(buf, bufsize) != TRANSPORT_SUCCESS)) {
            return TRANSPORT_ERROR;
        }

        memcpy(buf, &num_recs, sizeof(num_recs));
        (char *&) buf += sizeof(num_recs);
        bufsize -= sizeof(num_recs);

        memcpy(buf, &op, sizeof(op));
        (char *&) buf += sizeof(op);
        bufsize -= sizeof(op);

        return TRANSPORT_SUCCESS;
    }

    int unpack(void *&buf, std::size_t &bufsize) {
        if ((subject.deserialize(buf, bufsize)   != TRANSPORT_SUCCESS) ||
            (predicate.deserialize(buf, bufsize) != TRANSPORT_SUCCESS)) {
            return TRANSPORT_ERROR;
        }

        memcpy(&num_recs, buf, sizeof(num_recs));
        (char *&) buf += sizeof(num_recs);
        bufsize -= sizeof(num_recs);

        memcpy(&op, buf, sizeof(op));
        (char *&) buf += sizeof(op);
        bufsize -= sizeof(op);

        return TRANSPORT_SUCCESS;
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
