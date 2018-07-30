#ifndef TRANSPORT_REQUEST_HPP
#define TRANSPORT_REQUEST_HPP

#include "transport/Message.hpp"

namespace Transport {

namespace Request {

// messages sent by the client to the server
struct Request : Message {
    Request(Message::Type type);
    ~Request();

    virtual std::size_t size() const;
};

}
}

#endif
