#ifndef TRANSPORT_REQUEST_HPP
#define TRANSPORT_REQUEST_HPP

#include "transport/Messages/Message.hpp"

namespace Transport {

namespace Request {

// messages sent by the client to the server
struct Request : Message {
    Request(const enum hxhim_op_t type);
    virtual ~Request();

    virtual void alloc(const std::size_t max);
    virtual std::size_t add(const std::size_t ds, const bool increment_count);
    virtual int cleanup();

  protected:
    virtual int steal(Request *from, const std::size_t i);
};

}
}

#endif
