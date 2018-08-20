#include "transport/Messages/Bulk.hpp"
#include "transport/constants.h"

Transport::Bulk::Bulk(const std::size_t max)
    : ds_offsets(nullptr),
      count(0)
{
    alloc(max);
}

Transport::Bulk::~Bulk() {
    Bulk::cleanup();
}

int Transport::Bulk::alloc(const std::size_t max) {
    Bulk::cleanup();

    if (max) {
        if (!(ds_offsets = new int[max]())) {
            cleanup();
            return TRANSPORT_ERROR;
        }

        count = 0;
    }

    return TRANSPORT_SUCCESS;
}

int Transport::Bulk::cleanup() {
    delete [] ds_offsets;
    ds_offsets = nullptr;

    count = 0;

    return TRANSPORT_SUCCESS;
}
