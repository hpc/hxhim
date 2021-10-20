#include "transport/Messages/BHistogram.hpp"

Transport::Request::BHistogram::BHistogram(const std::size_t max)
    : Request(hxhim_op_t::HXHIM_HISTOGRAM),
      names(nullptr)
{
    alloc(max);
}

Transport::Request::BHistogram::~BHistogram() {
    cleanup();
}

void Transport::Request::BHistogram::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        Request::alloc(max);
        names = alloc_array<Blob>(max);
    }
}

std::size_t Transport::Request::BHistogram::add(Blob name) {
    names[count] = name;
    return Request::add(name.pack_size(false), true);
}

int Transport::Request::BHistogram::steal(Transport::Request::BHistogram *from, const std::size_t i) {
    if (Request::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BHistogram::cleanup() {
    dealloc_array(names, max_count);
    names = nullptr;

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

void Transport::Response::BHistogram::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        Response::alloc(max);
        histograms = alloc_array<std::shared_ptr<Histogram::Histogram> >(max);
    }
}

std::size_t Transport::Response::BHistogram::add(const std::shared_ptr<Histogram::Histogram> &hist, int status) {
    size_t ds = 0;
    if (status == DATASTORE_SUCCESS) {
        histograms[count] = hist;
        ds = hist->pack_size();
    }

    return Response::add(status, ds, true);
}

int Transport::Response::BHistogram::steal(Transport::Response::BHistogram *from, const std::size_t i) {
    add(std::move(from->histograms[i]), from->statuses[i]);
    from->histograms[i] = nullptr;

    return TRANSPORT_SUCCESS;
}

int Transport::Response::BHistogram::cleanup() {
    dealloc_array(histograms, max_count);
    histograms = nullptr;

    return Response::cleanup();
}
