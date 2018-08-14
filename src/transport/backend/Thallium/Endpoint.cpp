#include "transport/backend/Thallium/Endpoint.hpp"

std::mutex Transport::Thallium::Endpoint::mutex_ = {};

Transport::Thallium::Endpoint::Endpoint(const Engine_t &engine,
                                        const RPC_t &rpc,
                                        const Endpoint_t &ep)
    : ::Transport::Endpoint(),
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

Transport::Thallium::Endpoint::~Endpoint() {
    std::lock_guard<std::mutex> lock(mutex_);
}

Transport::Response::Put *Transport::Thallium::Endpoint::Put(const Request::Put *message) {
    return do_operation<Request::Put, Response::Put>(message);
}

Transport::Response::Get *Transport::Thallium::Endpoint::Get(const Request::Get *message) {
    return do_operation<Request::Get, Response::Get>(message);
}

Transport::Response::Delete *Transport::Thallium::Endpoint::Delete(const Request::Delete *message) {
    return do_operation<Request::Delete, Response::Delete>(message);
}

Transport::Response::Histogram *Transport::Thallium::Endpoint::Histogram(const Request::Histogram *message) {
    return do_operation<Request::Histogram, Response::Histogram>(message);
}
