#include "transport/Messages/Histogram.hpp"

Transport::Request::Histogram::Histogram(FixedBufferPool *fbp)
    : Request(Message::HISTOGRAM, fbp),
      Single()
{}

Transport::Request::Histogram::~Histogram()
{}

std::size_t Transport::Request::Histogram::size() const {
    return Request::size() + sizeof(ds_offset);
}

Transport::Response::Histogram::Histogram(FixedBufferPool *fbp)
    : Response(Message::HISTOGRAM, fbp),
      Single(),
      status(HXHIM_ERROR),
      hist()
{}

Transport::Response::Histogram::~Histogram()
{}

std::size_t Transport::Response::Histogram::size() const {
    return Response::size() + sizeof(ds_offset) +
        sizeof(status) + sizeof(hist.size) +
        ((sizeof(*hist.buckets) + sizeof(*hist.counts)) * hist.size);
}
