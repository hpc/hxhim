#include <cstring>
#include <stdexcept>
#include <utility>

#include "hxhim/Blob.hpp"
#include "utils/little_endian.hpp"
#include "utils/memory.hpp"

Blob::Blob(void *ptr, const std::size_t len, const hxhim_data_t type, const bool clean)
    : ptr(ptr),
      len(len),
      type(type),
      clean(clean)
{}

Blob::Blob(Blob &rhs)
    : Blob(rhs.ptr, rhs.len, rhs.type, false)
{}

Blob::Blob(Blob &&rhs)
    : Blob(rhs.ptr, rhs.len, rhs.type, rhs.clean)
{
    rhs.clear();
}

Blob::~Blob() {
    Blob::dealloc();
}

Blob& Blob::operator=(Blob &rhs) {
    ptr = rhs.ptr;
    len = rhs.len;
    type = rhs.type;
    clean = false;

    return *this;
}

Blob& Blob::operator=(Blob &&rhs) {
    if (this != &rhs) {
        ptr = rhs.ptr;
        len = rhs.len;
        type = rhs.type;
        clean = rhs.clean;

        rhs.clear();
    }

    return *this;
}

hxhim_data_t Blob::set_type(const hxhim_data_t new_type) {
    const hxhim_data_t old_type = type;
    type = new_type;
    return old_type;
}

bool Blob::set_clean(bool new_clean) {
    const bool old_clean = clean;
    clean = new_clean;
    return old_clean;
}

void Blob::clear() {
    ptr = nullptr;
    len = 0;
    type = hxhim_data_t::HXHIM_DATA_INVALID;
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
char *Blob::pack(char *&dst, const bool include_type) const {
    if (!dst || !ptr) {
        return nullptr;
    }

    little_endian::encode(dst, len);
    dst += sizeof(len);

    if (include_type) {
        little_endian::encode(dst, type);
        dst += sizeof(type);
    }

    memcpy(dst, ptr, len);
    dst += len;

    return dst;
}

std::size_t Blob::pack_size(const bool include_type) const {
    return pack_size(len, include_type);
}

std::size_t Blob::pack_size(const std::size_t len, const bool include_type) {
    return len + sizeof(len) + (include_type?sizeof(type):0);
}

// read from a blob of memory and create a deep copy
// (length is not known)
char *Blob::unpack(char *&src, const bool include_type) {
    if (!src) {
        throw std::runtime_error("unable to unpack blob");
    }

    little_endian::decode(len, src);
    src += sizeof(len);

    if (include_type) {
        little_endian::decode(type, src);
        src += sizeof(type);
    }

    ptr = alloc(len);
    memcpy(ptr, src, len);
    src += len;

    clean = true;

    return src;
}

// pack the ptr address and length
char *Blob::pack_ref(char *&dst, const bool include_type) const {
    if (!dst) {
        return nullptr;
    }

    memcpy(dst, &ptr, sizeof(ptr));
    dst += sizeof(ptr);

    little_endian::encode(dst, len);
    dst += sizeof(len);

    if (include_type) {
        little_endian::encode(dst, type);
        dst += sizeof(type);
    }

    return dst;
}

std::size_t Blob::pack_ref_size(const bool include_type) const {
    return sizeof(ptr) + sizeof(len) + (include_type?sizeof(type):0);
}

char *Blob::unpack_ref(char *&src, const bool include_type) {
    if (!src) {
        return nullptr;
    }

    memcpy(&ptr, src, sizeof(ptr));
    src += sizeof(ptr);

    little_endian::decode(len, src);
    src += sizeof(len);

    if (include_type) {
        little_endian::decode(type, src);
        src += sizeof(type);
    }

    clean = false;

    return src;
}

/**
 * get
 * Get values from a Blob in one function
 * Caller does not own pointer extracted from Blob
 *
 * @param addr     Address of variable to copy ptr into (optional)
 * @param length   how long the item stored in this Blob is (optional)
 * @param datatype the type of data pointed to by addr
 */
void Blob::get(void **addr, std::size_t *length, hxhim_data_t *datatype) const {
    if (addr) {
        *addr = ptr;
    }

    if (length) {
        *length = len;
    }

    if (datatype) {
        *datatype = type;
    }
}

void *Blob::data() const {
    return ptr;
}

std::size_t Blob::size() const {
    return len;
}

hxhim_data_t Blob::data_type() const {
    return type;
}

bool Blob::will_clean() const {
    return clean;
}

Blob::operator std::string() const {
    return std::string((char *) ptr, len);
}

// don't take ownership of ptr
Blob ReferenceBlob(void *ptr, const std::size_t len, const hxhim_data_t type) {
    return Blob(ptr, len, type, false);
}

// take ownership of ptr
Blob RealBlob(void *ptr, const std::size_t len, const hxhim_data_t type) {
    return Blob(ptr, len, type, true);
}

// length and data are known, but data needs to be copied
Blob RealBlob(const std::size_t len, const void *blob, const hxhim_data_t type) {
    if (!blob) {
        throw std::runtime_error("unable to unpack blob");
    }

    void *ptr = alloc(len);
    memcpy(ptr, blob, len);

    return Blob(ptr, len, type, true);
}
