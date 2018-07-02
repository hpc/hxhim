#include "utils/clone.hpp"

/**
 * _clone
 * Allocate space for, and copy the contents of src into *dst
 *
 * @param src     the source address
 * @param src_len the length of the source
 * @param dst     address of the destination pointer
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int _clone(void *src, std::size_t src_len, void **dst) {
    if (!src || !src_len ||
        !dst) {
        return MDHIM_ERROR;
    }

    if (!(*dst = ::operator new(src_len))) {
        return MDHIM_ERROR;
    }

    memcpy(*dst, src, src_len);
    return MDHIM_SUCCESS;
}

/**
 * _cleanup
 * Deallocate arrays
 *
 * @param data address of the data
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int _cleanup(void *data) {
    ::operator delete(data);
    return MDHIM_SUCCESS;
}

/**
 * _clone
 * Allocate space for, and copy the contents of srcs and src_lens into *dsts and *dst_lens
 *
 * @param srcs     the source addresses
 * @param src_lens the lengths of the source
 * @param dsts     addresses of the destination pointers
 * @param dst_lens address of the destination lengths
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int _clone(std::size_t count, void **srcs, std::size_t *src_lens, void ***dsts, std::size_t **dst_lens) {
    if (!count) {
        return MDHIM_SUCCESS;
    }

    if (!srcs || !src_lens ||
        !dsts || !dst_lens) {
        return MDHIM_ERROR;
    }

    *dsts = new void *[count]();
    *dst_lens = new std::size_t[count]();
    if (!*dsts || !*dst_lens) {
        delete [] *dsts;
        *dsts = nullptr;

        delete [] *dst_lens;
        *dst_lens = nullptr;

        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        if (!((*dsts)[i] = ::operator new(src_lens[i]))) {
            for(std::size_t j = 0; j < i; j++) {
                ::operator delete((*dsts)[j]);
            }

            delete [] *dsts;
            *dsts = nullptr;

            delete [] *dst_lens;
            *dst_lens = nullptr;

            return MDHIM_ERROR;
        }

        memcpy((*dsts)[i], srcs[i], src_lens[i]);
        (*dst_lens)[i] = src_lens[i];
    }

    return MDHIM_SUCCESS;
}

/**
 * _cleanup
 * Deallocate arrays of arrays and their lengths
 *
 * @param count how many data-len pairs there are
 * @param data  address of the data
 * @param len   the lengths of the data
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int _cleanup(std::size_t count, void **data, std::size_t *len) {
    if (data) {
        for(std::size_t i = 0; i < count; i++) {
            ::operator delete(data[i]);
        }
    }

    delete [] data;
    delete [] len;
    return MDHIM_SUCCESS;
}
