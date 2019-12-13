#ifndef TRANSPORT_BGET_HPP
#define TRANSPORT_BGET_HPP

#include "transport/messages/SPO.hpp"
#include "transport/messages/SP.hpp"
#include "transport/messages/Message.hpp"

namespace Transport {
namespace Request {

template <typename Blob_t>
class BGet : public virtual Message <Transport::SPO <Blob_t> > {
    public:
        BGet(const std::size_t max_count);
        BGet(void *buf, std::size_t bufsize);
};

}

namespace Response {

template <typename Blob_t>
class BGet : public virtual Message <Transport::SP <Blob_t> >{
    public:
        BGet(const std::size_t max_count);
        BGet(void *buf, std::size_t bufsize);
};

}

}

#include "transport/messages/BGet.tpp"

#endif
