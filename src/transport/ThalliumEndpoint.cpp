#include "ThalliumEndpoint.hpp"

std::mutex ThalliumEndpoint::mutex_ = {};
std::size_t ThalliumEndpoint::count_ = 0;
thallium::engine *ThalliumEndpoint::engine_ = nullptr;
thallium::remote_procedure *ThalliumEndpoint::rpc_ = nullptr;

ThalliumEndpoint::ThalliumEndpoint(thallium::engine *engine,
                                   thallium::remote_procedure *rpc,
                                   thallium::endpoint *ep)
  : TransportEndpoint(),
    ep_(ep)
{
    std::lock_guard<std::mutex> lock(mutex_);
    engine_ = engine;
    rpc_ = rpc;
    count_++;
}

ThalliumEndpoint::~ThalliumEndpoint() {
    std::lock_guard<std::mutex> lock(mutex_);

    Memory::FBP_MEDIUM::Instance().release(ep_);

    if (!--count_) {
        Memory::FBP_MEDIUM::Instance().release(rpc_);

        engine_->finalize();
        Memory::FBP_MEDIUM::Instance().release(engine_);
    }
}

TransportRecvMessage *ThalliumEndpoint::Put(const TransportPutMessage *message) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string buf;
    if (ThalliumPacker::pack(message, buf) != MDHIM_SUCCESS) {
        return nullptr;
    }

    std::string response = rpc_->on(*ep_)(buf);

    TransportRecvMessage *rm = nullptr;
    if (ThalliumUnpacker::unpack(&rm, response) != MDHIM_SUCCESS) {
        return nullptr;
    }

    return rm;
}

TransportGetRecvMessage *ThalliumEndpoint::Get(const TransportGetMessage *message) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string buf;
    if (ThalliumPacker::pack(message, buf) != MDHIM_SUCCESS) {
        return nullptr;
    }

    std::string response = rpc_->on(*ep_)(buf);

    TransportGetRecvMessage *rm = nullptr;
    if (ThalliumUnpacker::unpack(&rm, response) != MDHIM_SUCCESS) {
        return nullptr;
    }

    return rm;
}
