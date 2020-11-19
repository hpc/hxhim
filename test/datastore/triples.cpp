#include "triples.hpp"

Blob ReferenceBlob(const std::string &str, const hxhim_data_t type) {
    return Blob((void *) str.data(), str.size(), type, false);
}
