#include "transport/Messages/BHistogram.hpp"

Transport::Request::BHistogram::BHistogram(const std::size_t max)
    : Request(BHISTOGRAM)
{
    alloc(max);
}

Transport::Request::BHistogram::~BHistogram() {
    cleanup();
}

std::size_t Transport::Request::BHistogram::size() const {
    return Request::size();
}

int Transport::Request::BHistogram::alloc(const std::size_t max) {
    cleanup();
    return Request::alloc(max);
}

int Transport::Request::BHistogram::cleanup() {
    return Request::cleanup();
}

Transport::Response::BHistogram::BHistogram(const std::size_t max)
    : Response(BHISTOGRAM),
      hists(nullptr),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BHistogram::~BHistogram() {
    cleanup();
}

std::size_t Transport::Response::BHistogram::size() const {
    std::size_t ret = Response::size();
    for(std::size_t i = 0; i < count; i++) {
        ret += hists[i]->size() + sizeof(hists[i]->size());
    }
    return ret;
}

int Transport::Response::BHistogram::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Response::alloc(max) != TRANSPORT_SUCCESS) ||
            !(hists = alloc_array<RealBlob *>(max)))     {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BHistogram::cleanup() {
    for(std::size_t i = 0; i < count; i++) {
        destruct(hists[i]);
    }

    dealloc_array(hists, count);
    hists = nullptr;

    return Response::cleanup();
}
