#include "transport/Messages/Histogram.hpp"

Transport::Request::Histogram::Histogram(FixedBufferPool * arrays, FixedBufferPool *buffers)
    : Request(Message::HISTOGRAM, arrays, buffers),
      Single()
{}

Transport::Request::Histogram::~Histogram()
{}

std::size_t Transport::Request::Histogram::size() const {
    return Request::size() + sizeof(ds_offset);
}

Transport::Response::Histogram::Histogram(FixedBufferPool * arrays, FixedBufferPool *buffers)
    : Response(Message::HISTOGRAM, arrays, buffers),
      Single(),
      status(HXHIM_ERROR),
      hist()
{}

Transport::Response::Histogram::~Histogram()
{
    if (clean) {
        arrays->release_array(hist.buckets, hist.size);
        arrays->release_array(hist.counts, hist.size);
    }
}

std::size_t Transport::Response::Histogram::size() const {
    return Response::size() + sizeof(ds_offset) +
        sizeof(status) + sizeof(hist.size) +
        ((sizeof(*hist.buckets) + sizeof(*hist.counts)) * hist.size);
}
