#include "ThalliumEndpoint.hpp"

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
    std::lock_guard<std::mutex> lock(mutex_);

    if (!rpc_ || !ep_) {
        return nullptr;
    }

    std::string buf;
    if (ThalliumPacker::pack(message, buf) != MDHIM_SUCCESS) {
        return nullptr;
    }

    const std::string response = rpc_->on(*ep_)(buf);

    TransportRecvMessage *rm = nullptr;
    if (ThalliumUnpacker::unpack(&rm, response) != MDHIM_SUCCESS) {
        return nullptr;
    }

    return rm;
}

TransportGetRecvMessage *ThalliumEndpoint::Get(const TransportGetMessage *message) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!rpc_ || !ep_) {
        return nullptr;
    }

    std::string buf;
    if (ThalliumPacker::pack(message, buf) != MDHIM_SUCCESS) {
        return nullptr;
    }

    const std::string response = rpc_->on(*ep_)(buf);

    TransportGetRecvMessage *grm = nullptr;
    if (ThalliumUnpacker::unpack(&grm, response) != MDHIM_SUCCESS) {
        return nullptr;
    }

    return grm;
}
