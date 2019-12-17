#ifndef TRANSPORT_BLOB_HPP
#define TRANSPORT_BLOB_HPP

#include <cstdint>
#include <cstring>
#include <stdexcept>

#include "utils/memory.hpp"

/**
 * Blob
 * A simple struct to hold some data and its size.
 * The ptr is not deallocated.
 *
 * The values are intentionally left public to
 * allow for writing to them.
 */
struct Blob {
    Blob(void * ptr = nullptr, const std::size_t len = 0)
        : ptr(ptr),
          len(len)
    {}

    virtual ~Blob() {}

    char *pack(char *&blob) {
        if (!blob || (len && !ptr)) {
            return nullptr;
        }

        memcpy(blob, &len, sizeof(len));
        blob += sizeof(len);

        memcpy(blob, ptr, len);
        blob += len;

        return blob;
    }

    void *ptr;
    std::size_t len;
};

typedef Blob ReferenceBlob;

/**
 * RealBlob
 * Takes ownership of ptr and deallocates it upon destruction
 * Overwriting old values does not deallocate them.
 */
struct RealBlob : Blob {
    // take ownership of ptr
    RealBlob(void *ptr = nullptr, const std::size_t len = 0)
        : Blob(ptr, len)
    {}

    // read from a blob of memory and create a deep copy
    // (length is not known)
    RealBlob(char *&blob)
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

    // length is known, but data needs to be copied
    RealBlob(const std::size_t len, const void *blob = nullptr)
        : Blob(alloc(len), len)
    {
        if (blob) {
            memcpy(ptr, blob, len);
        }
    }

    RealBlob(const RealBlob &) = delete;
    RealBlob(RealBlob &&blob)
        : Blob(blob.ptr, blob.len)
    {
        blob.ptr = nullptr;
        blob.len = 0;
    }

    ~RealBlob() {
        dealloc(ptr);
    }

    RealBlob &operator=(const RealBlob &) = delete;
    RealBlob &operator=(RealBlob &&blob) {
        ptr = blob.ptr;
        len = blob.len;

        blob.ptr = nullptr;
        blob.len = 0;

        return *this;
    }
};

#endif
