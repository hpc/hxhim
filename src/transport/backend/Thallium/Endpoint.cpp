#if HXHIM_HAVE_THALLIUM

#include "transport/backend/Thallium/Endpoint.hpp"

std::mutex Transport::Thallium::Endpoint::mutex = {};

Transport::Thallium::Endpoint::Endpoint(const Engine_t &engine,
                                        const RPC_t &rpc,
                                        const Endpoint_t &ep,
                                        FixedBufferPool *responses,
                                        FixedBufferPool *arrays,
                                        FixedBufferPool *buffers)
  : ::Transport::Endpoint(),
    engine(engine),
    rpc(rpc),
    ep(ep),
    responses(responses),
    arrays(arrays),
    buffers(buffers)
{
    std::lock_guard<std::mutex> lock(mutex);

    if (!ep) {
        throw std::runtime_error("thallium::endpoint in ThalliumEndpoint must not be nullptr");
    }

    if (!rpc) {
        throw std::runtime_error("thallium::remote_procedure in ThalliumEndpoint must not be nullptr");
    }

    if (!responses) {
        throw std::runtime_error("FixedBufferPool in ThalliumEndpoint must not be nullptr");
    }

    if (!arrays) {
        throw std::runtime_error("FixedBufferPool in ThalliumEndpoint must not be nullptr");
    }

    if (!buffers) {
        throw std::runtime_error("FixedBufferPool in ThalliumEndpoint must not be nullptr");
    }
}

Transport::Thallium::Endpoint::~Endpoint() {
    std::lock_guard<std::mutex> lock(mutex);
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