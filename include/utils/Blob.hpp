#ifndef TRANSPORT_BLOB_HPP
#define TRANSPORT_BLOB_HPP

#include <cstdint>

/**
 * Blob
 * A simple struct to hold some data and its size.
 * The ptr is not deallocated.
 *
 * The values are intentionally left public to
 * allow for writing to them.
 */
struct Blob {
    Blob(void *ptr = nullptr, const std::size_t len = 0);

    virtual ~Blob();

    // read to a blob of memory
    // the blob argument is assumed to be defined and large enough to fit the data
    // (length is not known)
    char *pack(char *&blob);

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
    RealBlob(void *ptr = nullptr, const std::size_t len = 0);

    // read from a blob of memory and create a deep copy
    // (length is not known)
    RealBlob(char *&blob);

    // length and data are known, but data needs to be copied
    RealBlob(const std::size_t len, const void *blob);

    RealBlob(const RealBlob &) = delete;
    RealBlob(RealBlob &&blob);

    ~RealBlob();

    RealBlob &operator=(const RealBlob &) = delete;
    RealBlob &operator=(RealBlob &&blob);
};

#endif
