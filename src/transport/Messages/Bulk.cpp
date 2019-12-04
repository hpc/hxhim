#include "transport/Messages/Bulk.hpp"
#include "transport/constants.hpp"
#include "utils/memory.hpp"

Transport::Bulk::Bulk()
    : ds_offsets(nullptr),
      count(0),
      max_count(0)
{}

Transport::Bulk::~Bulk() {}

int Transport::Bulk::alloc(const std::size_t max) {
    Bulk::cleanup();

    if ((max_count = max)) {
        if (!(ds_offsets = alloc_array<int>(max))) {
            cleanup();
            return TRANSPORT_ERROR;
        }

        count = 0;
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Bulk::cleanup() {
    dealloc_array(ds_offsets, max_count);

    count = 0;

    return TRANSPORT_SUCCESS;
}
