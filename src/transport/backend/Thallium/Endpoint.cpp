#if HXHIM_HAVE_THALLIUM

#include "transport/backend/Thallium/Endpoint.hpp"

std::mutex Transport::Thallium::Endpoint::mutex_ = {};

Transport::Thallium::Endpoint::Endpoint(const Engine_t &engine,
                                        const RPC_t &rpc,
                                        const Endpoint_t &ep,
                                        FixedBufferPool *fbp)
  : ::Transport::Endpoint(),
    engine_(engine),
    rpc_(rpc),
    ep_(ep),
    fbp_(fbp)
{
    std::lock_guard<std::mutex> lock(mutex_);

    if (!ep_) {
        throw std::runtime_error("thallium::endpoint in ThalliumEndpoint must not be nullptr");
    }

    if (!rpc_) {
        throw std::runtime_error("thallium::remote_procedure in ThalliumEndpoint must not be nullptr");
    }

    if (!fbp_) {
        throw std::runtime_error("FixedBufferPool in ThalliumEndpoint must not be nullptr");
    }
}

Transport::Thallium::Endpoint::~Endpoint() {
    std::lock_guard<std::mutex> lock(mutex_);
}

Transport::Response::Put *Transport::Thallium::Endpoint::Put(const Request::Put *message) {
    return do_operation<Response::Put>(message);
}

Transport::Response::Get *Transport::Thallium::Endpoint::Get(const Request::Get *message) {
    return do_operation<Response::Get>(message);
}

Transport::Response::Delete *Transport::Thallium::Endpoint::Delete(const Request::Delete *message) {
    return do_operation<Response::Delete>(message);
}

Transport::Response::Histogram *Transport::Thallium::Endpoint::Histogram(const Request::Histogram *message) {
    return do_operation<Response::Histogram>(message);
}

#endif
