#ifndef RESPONSE_MESSAGE_HPP
#define RESPONSE_MESSAGE_HPP

#include "datastore/constants.hpp"
#include "message/Message.hpp"

namespace Message {

namespace Response {

// messages sent by the client to the server
struct Response : Message {
    Response(const enum hxhim_op_t type);
    virtual ~Response();

    virtual void alloc(const std::size_t max);
    virtual std::size_t add(const int status, const std::size_t ds, const bool increment_count);
    virtual int cleanup();

    int *statuses; // DATASTORE_SUCCESS or DATASTORE_ERROR

    Response *next;

  protected:
    virtual int steal(Response *from, const std::size_t i);
};

}
}

#endif
