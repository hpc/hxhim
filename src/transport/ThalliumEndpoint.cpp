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

        // engine_->finalize();
        Memory::FBP_MEDIUM::Instance().release(engine_);
    }
}

TransportRecvMessage *ThalliumEndpoint::Put(const TransportPutMessage *message) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string buf;
    if (ThalliumPacker::pack(message, buf) != MDHIM_SUCCESS) {
        return nullptr;
    }

    std::cout << (std::string) engine_->self() << " sending put to " << (std::string) *ep_ << std::endl;
    int response = rpc_->on(*ep_)(buf);
    std::cout << "put response " << response << std::endl;
    return nullptr;
}

TransportGetRecvMessage *ThalliumEndpoint::Get(const TransportGetMessage *message) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::cout << (std::string) engine_->self() << " sending get to " << (std::string) *ep_ << std::endl;
    std::string buf;
    if (ThalliumPacker::pack(message, buf) != MDHIM_SUCCESS) {
        return nullptr;
    }

    int response = rpc_->on(*ep_)(buf);
    std::cout << "get response " << response << std::endl;
    return nullptr;
}
