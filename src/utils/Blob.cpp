#include <cstring>
#include <stdexcept>

#include "utils/Blob.hpp"
#include "utils/big_endian.hpp"
#include "utils/memory.hpp"

Blob::Blob(void *ptr, const std::size_t len)
    : ptr(ptr),
      len(len)
{}

Blob::~Blob() {}

Blob::Blob(Blob * blob)
    : Blob(blob->ptr, blob->len)
{}

// read to a blob of memory
// the blob argument is assumed to be defined and large enough to fit the data
// (length is not known)
char *Blob::pack(char *&dst) const {
    if (!dst || !ptr) {
        return nullptr;
    }

    encode_unsigned(dst, len);
    dst += sizeof(len);

    memcpy(dst, ptr, len);
    dst += len;

    return dst;
}

std::size_t Blob::pack_size() const {
    return len + sizeof(len);
}

// pack the ptr address and length
char *Blob::pack_ref(char *&dst) const {
    if (!dst) {
        return nullptr;
    }

    memcpy(dst, &ptr, sizeof(ptr));
    dst += sizeof(ptr);

    encode_unsigned(dst, len);
    dst += sizeof(len);

    return dst;
}

std::size_t Blob::pack_ref_size() const {
    return sizeof(ptr) + sizeof(len);
}

/**
 * get
 * Get values from a Blob in one function
 * Caller does not own pointer extracted from Blob
 *
 * @param addr    Address of variable to copy ptr into (optional)
 * @param length  how long the item stored in this Blob is (optional)
 */
void Blob::get(void **addr, std::size_t *length) const {
    if (addr) {
        *addr = ptr;
    }

    if (length) {
        *length = len;
    }
}

void *Blob::data() const {
    return ptr;
}

std::size_t Blob::size() const {
    return len;
}

Blob::operator std::string() const {
    return std::string((char *) ptr, len);
}

ReferenceBlob::ReferenceBlob(void *ptr, const std::size_t len)
    : Blob(ptr, len)
{}

ReferenceBlob::ReferenceBlob(ReferenceBlob *blob)
    : Blob(blob)
{}

char *ReferenceBlob::unpack_ref(char *&src) {
    if (!src) {
        return nullptr;
    }

    memcpy(&ptr, src, sizeof(ptr));
    src += sizeof(ptr);

    decode_unsigned(len, src);
    src += sizeof(len);

    return src;
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

    decode_unsigned(len, blob);
    blob += sizeof(len);

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
