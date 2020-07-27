#ifndef TRANSPORT_BLOB_HPP
#define TRANSPORT_BLOB_HPP

#include <cstdint>
#include <string>

/**
 * Blob
 * A simple struct to hold some data and its size.
 * The ptr is not deallocated.
 *
 * The values are intentionally left public to
 * allow for writing to them.
 */
class Blob {
    public:
        Blob(void *ptr = nullptr, const std::size_t len = 0);
        Blob(Blob * blob);

        virtual ~Blob();

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
};

/**
 * ReferenceBlob
 * A Blob with the ability to unpack references
 * since doing so does not break pointer ownership
 */
class ReferenceBlob : public Blob {
    public:
        ReferenceBlob(void *ptr = nullptr, const std::size_t len = 0);
        ReferenceBlob(ReferenceBlob *blob);

        char *unpack_ref(char *&src);
};

/**
 * RealBlob
 * Takes ownership of ptr and deallocates it upon destruction
 * Overwriting old values does not deallocate them.
 */
class RealBlob : public Blob {
    public:
        // take ownership of ptr
        RealBlob(void *ptr = nullptr, const std::size_t len = 0);

        // read from a blob of memory and create a deep copy
        // (length is not known)
        RealBlob(char *&blob);

        // length and data are known, but data needs to be copied
        RealBlob(const std::size_t len, const void *blob);

        RealBlob(const RealBlob &) = delete;
        RealBlob(RealBlob *) = delete;

        RealBlob(RealBlob &&blob);

        ~RealBlob();

        RealBlob &operator=(const RealBlob &) = delete;
        RealBlob &operator=(RealBlob &&blob);
};

#endif
