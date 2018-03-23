#include <thallium/serialization/stl/string.hpp>

#include "ThalliumEndpoint.hpp"

ThalliumEndpoint::ThalliumEndpoint(const std::string &local_sender_protocol,
                                   const std::string &client_receiver_protocol,
                                   const std::string &local_receiver_protocol)
  : sender_(new thallium::engine(local_sender_protocol, THALLIUM_CLIENT_MODE)),
    client_protocol_(client_receiver_protocol),
    receiver_(new thallium::engine(local_receiver_protocol, THALLIUM_SERVER_MODE))
{}

ThalliumEndpoint::~ThalliumEndpoint() {
    sender_->finalize();
    delete sender_;

    receiver_->finalize();
    delete receiver_;
}

int ThalliumEndpoint::AddPutRequest(const TransportPutMessage *message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    std::string buf;
    if (ThalliumPacker::pack(message, buf) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    thallium::remote_procedure putrequest = sender_->define("send_rangesrv_work");
    thallium::endpoint server = sender_->lookup(client_protocol_);

    return putrequest.on(server)(buf);
}

int ThalliumEndpoint::AddGetRequest(const TransportGetMessage *message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    std::string buf;
    if (ThalliumPacker::pack(message, buf) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    thallium::remote_procedure getrequest = sender_->define("send_rangesrv_work");
    thallium::endpoint server = sender_->lookup(client_protocol_);

    return getrequest.on(server)(buf);
}

int ThalliumEndpoint::AddPutReply(TransportRecvMessage **message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    thallium::remote_procedure putreply = sender_->define("receive_response");
    thallium::endpoint server = sender_->lookup(client_protocol_);

    std::string buf = putreply.on(server)();
    return (ThalliumUnpacker::unpack(message, buf) != MDHIM_SUCCESS)?MDHIM_SUCCESS:MDHIM_ERROR;
}

int ThalliumEndpoint::AddGetReply(TransportGetRecvMessage **message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    thallium::remote_procedure getreply = sender_->define("receive_response");
    thallium::endpoint server = sender_->lookup(client_protocol_);

    std::string buf = getreply.on(server)();
    return (ThalliumUnpacker::unpack(message, buf) != MDHIM_SUCCESS)?MDHIM_SUCCESS:MDHIM_ERROR;
}
