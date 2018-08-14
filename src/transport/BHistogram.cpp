#include "transport/BHistogram.hpp"

Transport::Request::BHistogram::BHistogram(const std::size_t max)
    : Request(Message::BHISTOGRAM),
      Bulk()
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
        if (Bulk::alloc(max) != TRANSPORT_SUCCESS) {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BHistogram::cleanup() {
    Bulk::cleanup();

    return TRANSPORT_SUCCESS;
}

Transport::Response::BHistogram::BHistogram(const std::size_t max)
    : Response(Message::BHISTOGRAM),
      Bulk(),
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
            sizeof(hists[i].size()) + ((sizeof(double) + sizeof(std::size_t)) * hists[i].size());
    }
    return ret;
}

int Transport::Response::BHistogram::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        if ((Bulk::alloc(max) != TRANSPORT_SUCCESS)             ||
            !(statuses = new int[max]())                        ||
            !(hists = new std::map<double, std::size_t>[max]())) {
            cleanup();
            return TRANSPORT_ERROR;
        }
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BHistogram::cleanup() {
    delete [] statuses;
    statuses = nullptr;

    delete [] hists;
    hists = nullptr;

    Bulk::cleanup();

    return TRANSPORT_SUCCESS;
}
