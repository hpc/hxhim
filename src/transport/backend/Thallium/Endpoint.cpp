#if HXHIM_HAVE_THALLIUM

#include "transport/backend/Thallium/Endpoint.hpp"

/**
 * do_operation
 * A function containing the common calls used by Put, Get, and Delete
 *
 * @tparam message the message being sent
 * @treturn the response from the range server
 */
template <typename Send_t, typename Recv_t, typename = std::enable_if<std::is_base_of<Transport::Request::Request,   Send_t>::value &&
                                                                      std::is_base_of<Transport::Single,             Send_t>::value &&
                                                                      std::is_base_of<Transport::Response::Response, Recv_t>::value &&
                                                                      std::is_base_of<Transport::Single,             Recv_t>::value> >
    Recv_t *do_operation(const Send_t *message,
                         const Transport::Thallium::Engine_t &engine,
                         const Transport::Thallium::RPC_t &rpc,
                         const std::size_t buffer_size,
                         const Transport::Thallium::Endpoint_t &ep,
                         std::mutex & mutex) {
    std::lock_guard<std::mutex> lock(mutex);

    void *buf = nullptr;
    std::size_t bufsize = 0;
    if (Transport::Thallium::Packer::pack(message, &buf, &bufsize) != TRANSPORT_SUCCESS) {
        return nullptr;
    }

    std::vector<std::pair<void *, std::size_t> > segments = {std::make_pair(buf, buffer_size)};
    thallium::bulk bulk = engine->expose(segments, thallium::bulk_mode::read_write);
    const std::size_t response_size = rpc->on(*ep)(bulk);

    Recv_t *ret = nullptr;
    Transport::Thallium::Unpacker::unpack(&ret, buf, response_size); // no need to check return value
    dealloc(buf);
    return ret;
}

std::mutex Transport::Thallium::Endpoint::mutex = {};

Transport::Thallium::Endpoint::Endpoint(const Engine_t &engine,
                                        const RPC_t &rpc,
                                        const std::size_t buffer_size,
                                        const Endpoint_t &ep)
  : ::Transport::Endpoint(),
    engine(engine),
    rpc(rpc),
    buffer_size(buffer_size),
    ep(ep)
{
    std::lock_guard<std::mutex> lock(mutex);

    if (!rpc) {
        throw std::runtime_error("thallium::remote_procedure in ThalliumEndpoint must not be nullptr");
    }

    if (!ep) {
        throw std::runtime_error("thallium::endpoint in ThalliumEndpoint must not be nullptr");
    }
}

Transport::Thallium::Endpoint::~Endpoint() {
    std::lock_guard<std::mutex> lock(mutex);
}

Transport::Response::Put *Transport::Thallium::Endpoint::communicate(const Request::Put *message) {
    return do_operation<Request::Put, Response::Put>(message, engine, rpc, buffer_size, ep, mutex);
}

Transport::Response::Get *Transport::Thallium::Endpoint::communicate(const Request::Get *message) {
    return do_operation<Request::Get, Response::Get>(message, engine, rpc, buffer_size, ep, mutex);
}

Transport::Response::Get2 *Transport::Thallium::Endpoint::communicate(const Request::Get2 *message) {
    return do_operation<Request::Get2, Response::Get2>(message, engine, rpc, buffer_size, ep, mutex);
}

Transport::Response::Delete *Transport::Thallium::Endpoint::communicate(const Request::Delete *message) {
    return do_operation<Request::Delete, Response::Delete>(message, engine, rpc, buffer_size, ep, mutex);
}

Transport::Response::Histogram *Transport::Thallium::Endpoint::communicate(const Request::Histogram *message) {
    return do_operation<Request::Histogram, Response::Histogram>(message, engine, rpc, buffer_size, ep, mutex);
}

#endif
