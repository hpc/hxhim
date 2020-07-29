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
        char *unpack(char *&src);         // sets clean to true

        // pack the ptr address and length
        char *pack_ref(char *&dst) const;
        std::size_t pack_ref_size() const;
        char *unpack_ref(char *&src);     // sets clean to false

        // getters
        void get(void **addr, std::size_t *length) const;
        void *data() const;
        std::size_t size() const;
        bool will_clean() const;

        operator std::string() const;

    protected:
        void *ptr;
        std::size_t len;
        bool clean;         // whether or not to call dealloc in destructor
};

Blob ReferenceBlob(void *ptr = nullptr, const std::size_t len = 0);
Blob RealBlob(void *ptr = nullptr, const std::size_t len = 0);

// deep copy with known length
Blob RealBlob(const std::size_t len, const void *blob);

#endif
