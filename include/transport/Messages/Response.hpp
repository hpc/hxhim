#ifndef TRANSPORT_RESPONSE_HPP
#define TRANSPORT_RESPONSE_HPP

#include "datastore/constants.hpp"
#include "transport/Messages/Message.hpp"

namespace Transport {

namespace Response {

// messages sent by the client to the server
struct Response : Message {
    Response(const enum hxhim_op_t type);
    virtual ~Response();

    virtual std::size_t size() const;

    virtual int alloc(const std::size_t max);
    virtual int cleanup();

    int *statuses; // DATASTORE_SUCCESS or DATASTORE_ERROR

    Response *next;

  protected:
    virtual int steal(Response *from, const std::size_t i);
};

}
}

#endif
