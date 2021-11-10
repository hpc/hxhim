#ifndef REQUEST_MESSAGE_HPP
#define REQUEST_MESSAGE_HPP

#include "message/Message.hpp"

namespace Message {

namespace Request {

// messages sent by the client to the server
struct Request : Message {
    Request(const enum hxhim_op_t type);
    virtual ~Request();

    virtual void alloc(const std::size_t max);
    virtual std::size_t add(const std::size_t ds, const bool increment_count);
    virtual int cleanup();

    int dst_rank; // dst is a datastore ID - translate it to a rank here
};

}
}

#endif
