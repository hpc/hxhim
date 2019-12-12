#ifndef HXHIM_TRANSPORT_BPUT_HPP
#define HXHIM_TRANSPORT_BPUT_HPP

#include <stdexcept>

#include "hxhim/constants.h"
#include "transport/messages/SPO.hpp"
#include "transport/messages/SP.hpp"
#include "transport/messages/Message.hpp"

namespace Transport {
namespace Request {

template <typename Blob_t>
class BPut : public virtual Message <Transport::SPO <Blob_t> > {
    public:
        BPut(const std::size_t max_count);
        BPut(void *buf, std::size_t bufsize);
};

}

namespace Response {

template <typename Blob_t>
class BPut : public virtual Message <Transport::SP <Blob_t> >{
    public:
        BPut(const std::size_t max_count);
        BPut(void *buf, std::size_t bufsize);
};

}

}

#include "transport/messages/BPut.tpp"

#endif
