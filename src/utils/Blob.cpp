#include "utils/Blob.hpp"
#include "utils/memory.hpp"

#include <cstring>
#include <stdexcept>

Blob::Blob(void *ptr, const std::size_t len)
    : ptr(ptr),
      len(len)
{}

Blob::~Blob() {}

// read to a blob of memory
// the blob argument is assumed to be defined and large enough to fit the data
// (length is not known)
char *Blob::pack(char *&blob) {
    if (!blob || !ptr) {
        return nullptr;
    }

    memcpy(blob, &len, sizeof(len));
    blob += sizeof(len);

    memcpy(blob, ptr, len);
    blob += len;

    return blob;
}

// take ownership of ptr
RealBlob::RealBlob(void *ptr, const std::size_t len)
    : Blob(ptr, len)
{}

// read from a blob of memory and create a deep copy
// (length is not known)
RealBlob::RealBlob(char *&blob)
    : Blob(nullptr, 0)
{
    if (!blob) {
        throw std::runtime_error("unable to unpack blob");
    }

    memcpy(&len, blob, sizeof(len));
    blob += sizeof(std::size_t);

    ptr = alloc(len);
    memcpy(ptr, blob, len);
    blob += len;
}

// length and data are known, but data needs to be copied
RealBlob::RealBlob(const std::size_t len, const void *blob)
    : Blob(alloc(len), len)
{
    if (blob) {
        memcpy(ptr, blob, len);
    }
}

RealBlob::RealBlob(RealBlob &&blob)
    : Blob(blob.ptr, blob.len)
{
    blob.ptr = nullptr;
    blob.len = 0;
}

RealBlob::~RealBlob() {
    dealloc(ptr);
}

RealBlob &RealBlob::operator=(RealBlob &&blob) {
    ptr = blob.ptr;
    len = blob.len;

    blob.ptr = nullptr;
    blob.len = 0;

    return *this;
}
