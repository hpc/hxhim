#include "message/BHistogram.hpp"

Message::Request::BHistogram::BHistogram(const std::size_t max)
    : Request(hxhim_op_t::HXHIM_HISTOGRAM),
      names(nullptr)
{
    alloc(max);
}

Message::Request::BHistogram::~BHistogram() {
    cleanup();
}

void Message::Request::BHistogram::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        Request::alloc(max);
        names = alloc_array<Blob>(max);
    }
}

std::size_t Message::Request::BHistogram::add(Blob name) {
    names[count] = name;
    return Request::add(name.pack_size(false), true);
}

int Message::Request::BHistogram::cleanup() {
    dealloc_array(names, max_count);
    names = nullptr;

    return Request::cleanup();
}

Message::Response::BHistogram::BHistogram(const std::size_t max)
    : Response(hxhim_op_t::HXHIM_HISTOGRAM),
      histograms(nullptr)
{
    alloc(max);
}

Message::Response::BHistogram::~BHistogram() {
    cleanup();
}

void Message::Response::BHistogram::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        Response::alloc(max);
        histograms = alloc_array<std::shared_ptr<Histogram::Histogram> >(max);
    }
}

std::size_t Message::Response::BHistogram::add(const std::shared_ptr<Histogram::Histogram> &hist, int status) {
    size_t ds = 0;
    if (status == DATASTORE_SUCCESS) {
        histograms[count] = hist;
        ds = hist->pack_size();
    }

    return Response::add(status, ds, true);
}

int Message::Response::BHistogram::steal(BHistogram *from, const std::size_t i) {
    add(std::move(from->histograms[i]), from->statuses[i]);
    from->histograms[i] = nullptr;

    return MESSAGE_SUCCESS;
}

int Message::Response::BHistogram::cleanup() {
    dealloc_array(histograms, max_count);
    histograms = nullptr;

    return Response::cleanup();
}
