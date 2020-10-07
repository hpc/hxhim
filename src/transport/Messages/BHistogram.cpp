#include "transport/Messages/BHistogram.hpp"

Transport::Request::BHistogram::BHistogram(const std::size_t max)
    : Request(hxhim_op_t::HXHIM_HISTOGRAM)
{
    alloc(max);
}

Transport::Request::BHistogram::~BHistogram() {
    cleanup();
}

std::size_t Transport::Request::BHistogram::size() const {
    return Request::size();
}

void Transport::Request::BHistogram::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        Request::alloc(max);
    }
}

int Transport::Request::BHistogram::steal(Transport::Request::BHistogram *from, const std::size_t i) {
    if (Request::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BHistogram::cleanup() {
    return Request::cleanup();
}

Transport::Response::BHistogram::BHistogram(const std::size_t max)
    : Response(hxhim_op_t::HXHIM_HISTOGRAM),
      histograms(nullptr)
{
    alloc(max);
}

Transport::Response::BHistogram::~BHistogram() {
    cleanup();
}

std::size_t Transport::Response::BHistogram::size() const {
    std::size_t total = Response::size();
    for(std::size_t i = 0; i < count; i++) {
        total += histograms[i]->pack_size();
    }

    return total;
}

void Transport::Response::BHistogram::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        Response::alloc(max);
        histograms = alloc_array<std::shared_ptr<Histogram::Histogram> >(max);
    }
}

int Transport::Response::BHistogram::steal(Transport::Response::BHistogram *from, const std::size_t i) {
    if (Response::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    histograms[count] = from->histograms[i];

    count++;

    from->histograms[i] = nullptr;

    return HXHIM_SUCCESS;
}

int Transport::Response::BHistogram::cleanup() {
    dealloc_array(histograms, max_count);
    histograms = nullptr;

    return Response::cleanup();
}
