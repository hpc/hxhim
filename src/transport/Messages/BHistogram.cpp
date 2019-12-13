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
    return Request::size() + sizeof(count) + (sizeof(*ds_offsets) * count);
}

int Transport::Request::BHistogram::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if (Message::alloc(max) != TRANSPORT_SUCCESS) {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BHistogram::cleanup() {
    return TRANSPORT_SUCCESS;
}

Transport::Response::BHistogram::BHistogram(const std::size_t max)
    : Response(BHISTOGRAM),
      statuses(nullptr),
      hists(nullptr),
      next(nullptr)
{
    alloc(max);
}

Transport::Response::BHistogram::~BHistogram() {
    cleanup();
}

std::size_t Transport::Response::BHistogram::size() const {
    std::size_t ret = Response::size() + sizeof(count);
    for(std::size_t i = 0; i < count; i++) {
        ret += sizeof(ds_offsets[i]) + sizeof(statuses[i]) +
            sizeof(hists[i].size) + ((sizeof(*hists[i].buckets) + sizeof(*hists[i].counts)) * hists[i].size);
    }
    return ret;
}

int Transport::Response::BHistogram::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Message::alloc(max) != TRANSPORT_SUCCESS) ||
            !(statuses = alloc_array<int>(max))     ||
            !(hists = alloc_array<Histogram>(max)))  {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BHistogram::cleanup() {
    if (clean) {
        for(std::size_t i = 0; i < count; i++) {
            dealloc_array(hists[i].buckets, hists[i].size);
            dealloc_array(hists[i].counts, hists[i].size);
        }
    }

    dealloc_array(hists, count);
    hists = nullptr;

    dealloc_array(statuses, count);
    statuses = nullptr;

    return TRANSPORT_SUCCESS;
}
