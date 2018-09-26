#include <algorithm>

#include "datastore/datastore.hpp"
#include "hxhim/MaxSize.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/cache.hpp"
#include "hxhim/constants.h"
#include "transport/Messages/Messages.hpp"

std::size_t hxhim::MaxSize::Bulks() {
    static const std::size_t bulk_sizes[] = {
        sizeof(hxhim::PutData),
        sizeof(hxhim::GetData),
        sizeof(hxhim::GetOpData),
        sizeof(hxhim::DeleteData),
    };

    static const std::size_t max_size = *std::max_element(std::begin(bulk_sizes), std::end(bulk_sizes));

    return max_size;
}

std::size_t hxhim::MaxSize::Arrays() {
    static const std::size_t array_sizes[] =  {
        sizeof(void *),
        sizeof(void **),
        sizeof(int),
        sizeof(std::size_t),
        sizeof(hxhim_type_t),
        sizeof(Transport::Request::Put *),
        sizeof(Transport::Request::Get *),
        sizeof(Transport::Request::Delete *),
        sizeof(Transport::Request::Histogram *),
        sizeof(Transport::Request::BPut *),
        sizeof(Transport::Request::BGet *),
        sizeof(Transport::Request::BGetOp *),
        sizeof(Transport::Request::BDelete *),
        sizeof(Transport::Request::BHistogram *),
        sizeof(hxhim::datastore::Datastore *),
    };

    static const std::size_t max_size = *std::max_element(std::begin(array_sizes), std::end(array_sizes));

    return max_size;
}

std::size_t hxhim::MaxSize::Requests() {
    static const std::size_t request_sizes[] = {
        sizeof(Transport::Request::Put),
        sizeof(Transport::Request::Get),
        sizeof(Transport::Request::Delete),
        sizeof(Transport::Request::Histogram),
        sizeof(Transport::Request::BPut),
        sizeof(Transport::Request::BGet),
        sizeof(Transport::Request::BGetOp),
        sizeof(Transport::Request::BDelete),
        sizeof(Transport::Request::BHistogram),
    };

    static const std::size_t max_size = *std::max_element(std::begin(request_sizes), std::end(request_sizes));

    return max_size;
}

std::size_t hxhim::MaxSize::Responses() {
    static const std::size_t response_sizes[] = {
        sizeof(Transport::Response::Put),
        sizeof(Transport::Response::Get),
        sizeof(Transport::Response::Delete),
        sizeof(Transport::Response::Histogram),
        sizeof(Transport::Response::BPut),
        sizeof(Transport::Response::BGet),
        sizeof(Transport::Response::BGetOp),
        sizeof(Transport::Response::BDelete),
        sizeof(Transport::Response::BHistogram),
    };

    static const std::size_t max_size = *std::max_element(std::begin(response_sizes), std::end(response_sizes));

    return max_size;
}

std::size_t hxhim::MaxSize::Result() {
    static const std::size_t result_sizes[] = {
        sizeof(hxhim::Results::Put),
        sizeof(hxhim::Results::Get),
        sizeof(hxhim::Results::Delete),
        sizeof(hxhim::Results::Sync),
        sizeof(hxhim::Results::Histogram),
    };

    static const std::size_t max_size = *std::max_element(std::begin(result_sizes), std::end(result_sizes));

    return max_size;
}
