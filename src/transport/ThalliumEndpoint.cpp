#include "transport/ThalliumEndpoint.hpp"

std::mutex ThalliumEndpoint::mutex_ = {};

ThalliumEndpoint::ThalliumEndpoint(const Thallium::Engine_t &engine,
                                   const Thallium::RPC_t &rpc,
                                   const Thallium::Endpoint_t &ep)
  : TransportEndpoint(),
    engine_(engine),
    rpc_(rpc),
    ep_(ep)
{
    std::lock_guard<std::mutex> lock(mutex_);

    if (!ep_) {
        throw std::runtime_error("thallium::endpoint in ThalliumEndpoint must not be nullptr");
    }

    if (!rpc_) {
        throw std::runtime_error("thallium::remote_procedure in ThalliumEndpoint must not be nullptr");
    }
}

ThalliumEndpoint::~ThalliumEndpoint() {
    std::lock_guard<std::mutex> lock(mutex_);
}

TransportRecvMessage *ThalliumEndpoint::Put(const TransportPutMessage *message) {
    return do_operation<TransportPutMessage, TransportRecvMessage>(message);
}

TransportGetRecvMessage *ThalliumEndpoint::Get(const TransportGetMessage *message) {
    return do_operation<TransportGetMessage, TransportGetRecvMessage>(message);
}

TransportRecvMessage *ThalliumEndpoint::Delete(const TransportDeleteMessage *message) {
    return do_operation<TransportDeleteMessage, TransportRecvMessage>(message);
}
