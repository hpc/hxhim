#ifndef BLOB_HPP
#define BLOB_HPP

#include <cstdint>
#include <ostream>
#include <string>

#include "hxhim/constants.h"

/**
 * Blob
 * A structure for holding pointers
 * and enforcing ownership semantics.
 *
 * If clean is false, *this is a reference
 * to the original data.
 *
 * If clean is true, *this blob has taken
 * ownership of the pointer, and will
 * deallocate the pointer with the dealloc
 * function found in utils/memory.hpp
 * when *this blob is overwritten
 * or destructed.
 */
class Blob {
    public:
        // save ptr but do not touch data
        Blob(void *ptr = nullptr, const std::size_t len = 0,
             const hxhim_data_t type = hxhim_data_t::HXHIM_DATA_INVALID,
             const bool clean = false);

        // memcpy the ptr
        Blob(const std::size_t len, const void *ptr,
             const hxhim_data_t type = hxhim_data_t::HXHIM_DATA_INVALID);

        // reference to a string
        Blob(const std::string &str);

        // creates a reference to rhs
        Blob(Blob &rhs);

        // moves rhs to here
        Blob(Blob &&rhs);

        virtual ~Blob();

        // destroy this and reference rhs
        Blob &operator=(const Blob &rhs);

        // destroy this, take rhs, and clear rhs
        Blob &operator=(Blob &&rhs);

        // compare ptr, len, and type
        // clean is not compared
        bool operator==(const Blob &rhs) const;
        bool operator!=(const Blob &rhs) const;

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

        friend std::ostream &operator<<(std::ostream &stream, const Blob &blob);

    protected:
        void *ptr;
        std::size_t len;
        hxhim_data_t type;
        bool clean;         // whether or not to call dealloc in destructor
};

// Convenience wrapper for constructing references
Blob ReferenceBlob(void *ptr, const std::size_t len,
                   const hxhim_data_t type);

// Convenience wrapper for constructing Blobs that own their pointers
Blob RealBlob(void *ptr, const std::size_t len,
              const hxhim_data_t type);

// Convenience wrapper for constructing Blobs that copy the pointer
Blob RealBlob(const std::size_t len, const void *ptr,
              const hxhim_data_t type);

#endif
