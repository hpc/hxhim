#ifndef TRANSPORT_RESPONSE_HPP
#define TRANSPORT_RESPONSE_HPP

#include "transport/Messages/Message.hpp"

namespace Transport {

namespace Response {

// messages sent by the client to the server
struct Response : Message {
    Response(Message::Type type, FixedBufferPool *arrays, FixedBufferPool *buffers);
    ~Response();

    virtual std::size_t size() const;
};

}
}

#endif