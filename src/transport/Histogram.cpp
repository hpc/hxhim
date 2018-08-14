#include "transport/Histogram.hpp"

Transport::Request::Histogram::Histogram()
    : Request(Message::HISTOGRAM),
      Single()
{}

Transport::Request::Histogram::~Histogram()
{}

std::size_t Transport::Request::Histogram::size() const {
    return Request::size() + sizeof(ds_offset);
}

Transport::Response::Histogram::Histogram(const std::map<double, std::size_t> &hist)
    : Response(Message::HISTOGRAM),
      Single(),
      status(HXHIM_ERROR),
      hist(hist)
{}

Transport::Response::Histogram::~Histogram()
{}

std::size_t Transport::Response::Histogram::size() const {
    return Response::size() + sizeof(ds_offset) +
        sizeof(status) + sizeof(hist.size()) +
        ((sizeof(double) + sizeof(std::size_t)) * hist.size());
}
