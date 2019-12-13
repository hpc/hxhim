#ifndef TRANSPORT_BDELETE_HPP
#define TRANSPORT_BDELETE_HPP

#include "transport/messages/SP.hpp"
#include "transport/messages/Message.hpp"

namespace Transport {
namespace Request {

template <typename Blob_t>
class BDelete : public virtual Message <Transport::SP <Blob_t> > {
    public:
        BDelete(const std::size_t max_count);
        BDelete(void *buf, std::size_t bufsize);
};

}

namespace Response {

template <typename Blob_t>
class BDelete : public virtual Message <Transport::SP <Blob_t> >{
    public:
        BDelete(const std::size_t max_count);
        BDelete(void *buf, std::size_t bufsize);
};

}

}

#include "transport/messages/BDelete.tpp"

#endif
