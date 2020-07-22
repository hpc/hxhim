#ifndef TRANSPORT_REQUEST_HPP
#define TRANSPORT_REQUEST_HPP

#include "transport/Messages/Message.hpp"

namespace Transport {

namespace Request {

// messages sent by the client to the server
struct Request : Message {
    Request(const enum hxhim_op_t type);
    ~Request();

    virtual std::size_t size() const;

    virtual int alloc(const std::size_t max);
    virtual int cleanup();

  protected:
    int steal(Request *from, const std::size_t i);
};

}
}

#endif
