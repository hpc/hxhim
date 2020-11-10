#include <cstring>
#include <stdexcept>
#include <utility>

#include "utils/Blob.hpp"
#include "utils/little_endian.hpp"
#include "utils/memory.hpp"

Blob::Blob(void *ptr, const std::size_t len, const bool clean)
    : ptr(ptr),
      len(len),
      clean(clean)
{}

Blob::Blob(Blob &rhs)
    : Blob(rhs.ptr, rhs.len, rhs.clean)
{}

Blob::Blob(Blob &&rhs)
    : Blob(rhs.ptr, rhs.len, rhs.clean)
{
    rhs.clear();
}

Blob::~Blob() {
    Blob::dealloc();
}

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

bool Blob::set_clean(bool new_clean) {
    const bool old_clean = clean;
    clean = new_clean;
    return old_clean;
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

    little_endian::encode(dst, len);
    dst += sizeof(len);

    memcpy(dst, ptr, len);
    dst += len;

    return dst;
}

std::size_t Blob::pack_size() const {
    return pack_size(len);
}

std::size_t Blob::pack_size(const std::size_t len) {
    return len + sizeof(len);
}

// read from a blob of memory and create a deep copy
// (length is not known)
char *Blob::unpack(char *&src) {
    if (!src) {
        throw std::runtime_error("unable to unpack blob");
    }

    little_endian::decode(len, src);
    src += sizeof(len);

    ptr = alloc(len);
    memcpy(ptr, src, len);
    src += len;

    clean = true;

    return src;
}

// pack the ptr address and length
char *Blob::pack_ref(char *&dst) const {
    if (!dst) {
        return nullptr;
    }

    memcpy(dst, &ptr, sizeof(ptr));
    dst += sizeof(ptr);

    little_endian::encode(dst, len);
    dst += sizeof(len);

    return dst;
}

std::size_t Blob::pack_ref_size() const {
    return sizeof(ptr) + sizeof(len);
}

char *Blob::unpack_ref(char *&src) {
    if (!src) {
        return nullptr;
    }

    memcpy(&ptr, src, sizeof(ptr));
    src += sizeof(ptr);

    little_endian::decode(len, src);
    src += sizeof(len);

    clean = false;

    return src;
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

bool Blob::will_clean() const {
    return clean;
}

Blob::operator std::string() const {
    return std::string((char *) ptr, len);
}

// don't take ownership of ptr
Blob ReferenceBlob(void *ptr, const std::size_t len) {
    return Blob(ptr, len, false);
}

// take ownership of ptr
Blob RealBlob(void *ptr, const std::size_t len) {
    return Blob(ptr, len, true);
}

// length and data are known, but data needs to be copied
Blob RealBlob(const std::size_t len, const void *blob) {
    if (!blob) {
        throw std::runtime_error("unable to unpack blob");
    }

    void *ptr = alloc(len);
    memcpy(ptr, blob, len);

    return Blob(ptr, len, true);
}
