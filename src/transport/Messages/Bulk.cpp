#include "transport/Messages/Bulk.hpp"
#include "transport/constants.hpp"

Transport::Bulk::Bulk(const std::size_t max)
    : ds_offsets(nullptr),
      count(0)
{}

Transport::Bulk::~Bulk() {}

int Transport::Bulk::alloc(const std::size_t max, FixedBufferPool *arrays) {
    Bulk::cleanup(arrays);

    if (max) {
        if (!(ds_offsets = arrays->acquire_array<int>(max))) {
            cleanup();
            return TRANSPORT_ERROR;
        }

        count = 0;
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Bulk::cleanup(FixedBufferPool *arrays) {
    arrays->release_array(ds_offsets, count);
    ds_offsets = nullptr;

    count = 0;

    return TRANSPORT_SUCCESS;
}
