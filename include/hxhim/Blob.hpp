#ifndef TRANSPORT_BLOB_HPP
#define TRANSPORT_BLOB_HPP

#include <cstdint>
#include <string>

#include "hxhim/constants.h"

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
        Blob(void *ptr = nullptr, const std::size_t len = 0,
             const hxhim_data_t type = hxhim_data_t::HXHIM_DATA_BYTE, const bool clean = false);
        Blob(Blob &rhs);
        Blob(Blob &&rhs);

        virtual ~Blob();

        Blob &operator=(Blob &rhs);
        Blob &operator=(Blob &&rhs);

        hxhim_data_t set_type(const hxhim_data_t new_type);
        bool set_clean(bool new_clean);

        void clear();   // only null
        void dealloc(); // only deallocate if clean == true, and then null

        // read to a blob of memory
        // the blob argument is assumed to be defined and large enough to fit the data
        // (length is not known)
        char *pack(char *&dst, const bool include_type) const;
        std::size_t pack_size(const bool include_type) const;
        static std::size_t pack_size(const std::size_t len, const bool include_type);
        char *unpack(char *&src, const bool include_type);    // sets clean to true

        // pack the ptr address and length
        char *pack_ref(char *&dst, const bool include_type) const;
        std::size_t pack_ref_size(const bool include_type) const;
        char *unpack_ref(char *&src, const bool include_type);     // sets clean to false

        // getters
        void get(void **addr, std::size_t *length, hxhim_data_t *datatype) const;
        void *data() const;
        std::size_t size() const;
        hxhim_data_t data_type() const;
        bool will_clean() const;

        operator std::string() const;

    protected:
        void *ptr;
        std::size_t len;
        hxhim_data_t type;
        bool clean;         // whether or not to call dealloc in destructor
};

Blob ReferenceBlob(void *ptr, const std::size_t len,
                   const hxhim_data_t type);
Blob RealBlob(void *ptr, const std::size_t len,
              const hxhim_data_t type);

// deep copy with known length
Blob RealBlob(const std::size_t len, const void *blob,
              const hxhim_data_t type);

#endif
