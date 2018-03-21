#include "ThalliumEndpoint.hpp"

ThalliumEndpoint::ThalliumEndpoint(const std::string &sender_protocol,
                                   const std::string &receiver_protocol)
  : sender_(new thallium::engine(sender_protocol, THALLIUM_CLIENT_MODE)),
    receiver_protocol_(receiver_protocol)
{}

ThalliumEndpoint::~ThalliumEndpoint() {
    sender_->finalize();
    delete sender_;
}

int ThalliumEndpoint::AddPutRequest(const TransportPutMessage *message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    thallium::remote_procedure putrequest = sender_->define("putrequest");
    thallium::endpoint server = sender_->lookup(receiver_protocol_);

    // return putrequest.on(server)();
    return MDHIM_ERROR;
}

int ThalliumEndpoint::AddGetRequest(const TransportGetMessage *message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    thallium::remote_procedure getrequest = sender_->define("gettrequest");
    thallium::endpoint server = sender_->lookup(receiver_protocol_);

    // return getrequest.on(server)();
    return MDHIM_ERROR;
}

int ThalliumEndpoint::AddPutReply(TransportRecvMessage **message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    thallium::remote_procedure putreply = sender_->define("putreply");
    thallium::endpoint server = sender_->lookup(receiver_protocol_);

    // return (*message = putreply.on(server)())?MDHIM_SUCCESS:MDHIM_ERROR;
    return MDHIM_ERROR;
}

int ThalliumEndpoint::AddGetReply(TransportGetRecvMessage **message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    thallium::remote_procedure getreply = sender_->define("gettreply");
    thallium::endpoint server = sender_->lookup(receiver_protocol_);

    // return (*message = getreply.on(server)())?MDHIM_SUCCESS:MDHIM_ERROR;
    return MDHIM_ERROR;
}
