#include "transport/Messages/Histogram.hpp"
#include "utils/memory.hpp"

Transport::Request::Histogram::Histogram()
    : Request(HISTOGRAM),
      Single()
{}

Transport::Request::Histogram::~Histogram()
{}

std::size_t Transport::Request::Histogram::size() const {
    return Request::size() + sizeof(ds_offset);
}

Transport::Response::Histogram::Histogram()
    : Response(HISTOGRAM),
      Single(),
      status(HXHIM_ERROR),
      hist()
{}

Transport::Response::Histogram::~Histogram()
{
    if (clean) {
        dealloc_array(hist.buckets, hist.size);
        dealloc_array(hist.counts, hist.size);
    }
}

std::size_t Transport::Response::Histogram::size() const {
    return Response::size() + sizeof(ds_offset) +
        sizeof(status) + sizeof(hist.size) +
        ((sizeof(*hist.buckets) + sizeof(*hist.counts)) * hist.size);
}
