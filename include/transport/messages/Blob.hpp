#ifndef TRANSPORT_BLOB_HPP
#define TRANSPORT_BLOB_HPP

#include <iostream>
#include <cstring>

#include "transport/constants.hpp"
#include "utils/memory.hpp"

namespace Transport {

/**
 * Base type for pairs of data and length
 */
class Blob {
    public:
        Blob(void *buf = nullptr, const std::size_t length = 0, const std::size_t bufsize = 0)
            : ptr(buf),
              len(length),
              size(bufsize)
        {}

        Blob(const Blob &) = default;
        Blob(Blob &&) = default;

        virtual ~Blob() {}

        Blob &operator=(const Blob &) = default;
        Blob &operator=(Blob &&) = default;

        /**
         * Write the contents of this blob into a buffer
         *
         * @param buf     the target buffer          - position is updated
         * @param bufsize the length of the buffer   - value is updated
         * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
         */
         int serialize(void *&buf, std::size_t &bufsize) const {
            if (!buf || !ptr) {
                return TRANSPORT_ERROR;
            }

            const std::size_t total = total_size();
            if (total > bufsize) {
                return TRANSPORT_ERROR;
            }

            memcpy(buf, &len, sizeof(len));
            (char *&) buf += sizeof(len);
            memcpy(buf, ptr, len);
            (char *&) buf += len;

            bufsize -= total;
            return TRANSPORT_SUCCESS;
        }

        /**
         * Read a blob out of a buffer
         *
         * @param buf  the source buffer (encoded length + data)                   - position is updated
         * @param bufsize the length of the buffer (not the length of the data)    - value is updated
         * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
         */
        virtual int deserialize(void *&buf, std::size_t &bufsize) {
            if (!buf || !ptr) {
                return TRANSPORT_ERROR;
            }

            if (bufsize < sizeof(len)) {
                return TRANSPORT_ERROR;
            }

            // get the length of the following data
            memcpy(&len, buf, sizeof(len));
            if (total_size() > bufsize) {
                return TRANSPORT_ERROR;
            }

            bufsize -= sizeof(len);
            (char *&) buf += sizeof(len);

            memcpy(ptr, buf, len);
            bufsize -= len;
            (char *&) buf += len;

            return TRANSPORT_SUCCESS;
        }

        std::size_t total_size() const {
            return sizeof(len) + len;
        }

    protected:
    public:
        void *ptr;         // address of data
        std::size_t len;   // length of data in the buffer
        std::size_t size;  // size of the buffer
};

/**
 * A blob of data that is a reference to an external source.
 * A Reference blob does not own its data
 */
class ReferenceBlob : public Blob {
    public:
        ReferenceBlob(void *ext = nullptr, const std::size_t len = 0)
            : Blob(ext, len)
        {}
};

/**
 * A blob of data that is deep copy of the data it was given
 * A DeepCopyBlob owns its data
 */
class DeepCopyBlob : public Blob {
    public:
        DeepCopyBlob(void *buf = nullptr, const std::size_t length = 0)
            : Blob()
        {
            if (buf && length) {
                ptr = alloc(length);
                len = length;
                size = len;
                memcpy(ptr, buf, len);
            }
        }

        DeepCopyBlob(const DeepCopyBlob &blob)
            : DeepCopyBlob(blob.ptr, blob.len)
        {}

        DeepCopyBlob(DeepCopyBlob &&) = default;

        ~DeepCopyBlob() {
            cleanup();
        }

        DeepCopyBlob &operator=(const ReferenceBlob &blob) {
            cleanup();
            ptr = alloc(blob.len);
            len = blob.len;
            size = blob.size;
            memcpy(ptr, blob.ptr, len);
            return *this;
        }

        DeepCopyBlob &operator=(ReferenceBlob &&blob) {
            cleanup();

            // can't steal ownership, so have to do memcpy
            ptr = alloc(blob.len);
            len = blob.len;
            size = blob.size;
            memcpy(ptr, blob.ptr, len);

            blob.ptr = nullptr;
            blob.len = 0;
            blob.size = 0;
            return *this;
        }

        DeepCopyBlob &operator=(const DeepCopyBlob &blob) {
            cleanup();
            ptr = alloc(blob.len);
            len = blob.len;
            size = blob.size;
            memcpy(ptr, blob.ptr, len);
            return *this;
        }

        DeepCopyBlob &operator=(DeepCopyBlob &&blob) {
            cleanup();
            ptr = blob.ptr;
            len = blob.len;
            size = blob.size;
            blob.ptr = nullptr;
            blob.len = 0;
            blob.size = 0;
            return *this;
        }

        int deserialize(void *&buf, std::size_t &bufsize) {
            // allocate new buffer to place data in
            // the buffer stays allocated even if deserialization failed
            cleanup();
            ptr = alloc(bufsize);
            len = 0;
            size = bufsize;

            return Blob::deserialize(buf, size);
        }

        void cleanup() {
            dealloc(ptr);
            ptr = nullptr;

            len = 0;
            size = 0;
        }
};

}

#endif
