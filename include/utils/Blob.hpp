#ifndef TRANSPORT_BLOB_HPP
#define TRANSPORT_BLOB_HPP

#include <cstdint>
#include <string>

/**
 * Blob
 * A simple struct to hold some data and its size.
 * The ptr is not deallocated.
 *
 * If the rhs is a reference, it is copied
 * If the rhs is real, it is moved
 * lhs is always overwritten with no reguards to its type
 */
class Blob {
    public:
        Blob(void *ptr = nullptr, const std::size_t len = 0, const bool clean = false);
        Blob(Blob &rhs);
        Blob(Blob &&rhs);
        Blob(Blob *rhs);

        virtual ~Blob();

        Blob &operator=(Blob &rhs);
        Blob &operator=(Blob &&rhs);

        void clear();   // only null
        void dealloc(); // only deallocate if clean == true, and then null

        // read to a blob of memory
        // the blob argument is assumed to be defined and large enough to fit the data
        // (length is not known)
        char *pack(char *&dst) const;
        std::size_t pack_size() const;

        // pack the ptr address and length
        char *pack_ref(char *&dst) const;
        std::size_t pack_ref_size() const;

        // getters
        void get(void **addr, std::size_t *length) const;
        void *data() const;
        std::size_t size() const;

        operator std::string() const;

    protected:
        void *ptr;
        std::size_t len;
        bool clean;         // whether or not to call dealloc in destructor
};

/**
 * ReferenceBlob
 * Does not have ownership of its pointer
 */
class ReferenceBlob : public Blob {
    public:
        ReferenceBlob(void *ptr = nullptr, const std::size_t len = 0);
        ReferenceBlob(ReferenceBlob &rhs);
        ReferenceBlob(ReferenceBlob &&rhs);
        ReferenceBlob(ReferenceBlob *rhs);

        ReferenceBlob &operator=(ReferenceBlob &rhs);
        ReferenceBlob &operator=(ReferenceBlob &&rhs);

        char *unpack_ref(char *&src);
};

/**
 * RealBlob
 * Has ownership of its pointer
 */
class RealBlob : public Blob {
    public:
        // take ownership of ptr and deallocate it when destructing
        RealBlob(void *ptr = nullptr, const std::size_t len = 0);

        // rhs.ptr is set to nullptr
        RealBlob(RealBlob &rhs);
        RealBlob(RealBlob &&rhs);

        // read from a blob of memory and create a deep copy
        // (length is not known)
        RealBlob(char *&rhs);

        // length and data are known, but data needs to be copied
        RealBlob(const std::size_t len, const void *blob);

        RealBlob &operator=(RealBlob &rhs);
        RealBlob &operator=(RealBlob &&rhs);
};

#endif
