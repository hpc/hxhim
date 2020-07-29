#include <cstring>
#include <stdexcept>
#include <utility>

#include "utils/Blob.hpp"
#include "utils/big_endian.hpp"
#include "utils/memory.hpp"

Blob::Blob(void *ptr, const std::size_t len, const bool clean)
    : ptr(ptr),
      len(len),
      clean(clean)
{}

// equivalent to the move constructor
Blob::Blob(Blob &rhs)
    : Blob(std::move(rhs))
{}

Blob::Blob(Blob &&rhs)
    : Blob(rhs.ptr, rhs.len, rhs.clean)
{
    rhs.clear();
}

Blob::Blob(Blob *rhs)
    : Blob(*rhs)
{}

Blob::~Blob() {
    Blob::dealloc();
}

// equivalent to move assignment
Blob& Blob::operator=(Blob &rhs) {
    return (*this = std::move(rhs));
}

Blob& Blob::operator=(Blob &&rhs) {
    if (this != &rhs) {
        ptr = rhs.ptr;
        len = rhs.len;
        clean = rhs.clean;

        if (rhs.clean) {
            rhs.clear();
        }
    }

    return *this;
}

void Blob::clear() {
    ptr = nullptr;
    len = 0;
    clean = false;
}

void Blob::dealloc() {
    if (clean) {
        ::dealloc(ptr);
    }
    clear();
}

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
    : Blob(ptr, len, false)
{}

ReferenceBlob::ReferenceBlob(ReferenceBlob &rhs)
    : Blob(rhs)
{}

ReferenceBlob::ReferenceBlob(ReferenceBlob &&rhs)
    : Blob(rhs)
{}

ReferenceBlob::ReferenceBlob(ReferenceBlob *rhs)
    : Blob(rhs)
{}

ReferenceBlob &ReferenceBlob::operator=(ReferenceBlob &rhs) {
    return (*this = std::move(rhs));
}

ReferenceBlob &ReferenceBlob::operator=(ReferenceBlob &&rhs) {
    Blob::operator=(rhs);
    return *this;
}

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
    : Blob(ptr, len, true)
{}

RealBlob::RealBlob(RealBlob &rhs)
    : Blob(rhs)
{}

RealBlob::RealBlob(RealBlob &&rhs)
    : Blob(rhs)
{}

// read from a blob of memory and create a deep copy
// (length is not known)
RealBlob::RealBlob(char *&rhs)
    : Blob(nullptr, 0, true)
{
    if (!rhs) {
        throw std::runtime_error("unable to unpack blob");
    }

    decode_unsigned(len, rhs);
    rhs += sizeof(len);

    ptr = alloc(len);
    memcpy(ptr, rhs, len);
    rhs += len;
}

// length and data are known, but data needs to be copied
RealBlob::RealBlob(const std::size_t len, const void *blob)
    : Blob(alloc(len), len, true)
{
    if (blob) {
        memcpy(ptr, blob, len);
    }
}

RealBlob &RealBlob::operator=(RealBlob &rhs) {
    return (*this = std::move(rhs));
}

RealBlob &RealBlob::operator=(RealBlob &&rhs) {
    Blob::operator=(rhs);
    return *this;
}
