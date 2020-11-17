#ifndef DATASTORE_TRANSFORM_TPP
#define DATASTORE_TRANSFORM_TPP

#include "datastore/constants.hpp"
#include "hxhim/constants.h"
#include "utils/elen.hpp"
#include "utils/memory.hpp"
#include "utils/type_traits.hpp"

namespace datastore {
namespace Transform {
namespace encode {

template <typename T, typename = enable_if_t <std::is_integral<T>::value> >
int integers(void *src, const size_t,
             void **dst, size_t *dst_size,
             void *extra) {
    if (!src || !dst || !dst_size || !extra) {
        return DATASTORE_ERROR;
    }

    NumericExtra *num_extra = (NumericExtra *) extra;

    const std::string encoded = elen::encode::integers(* (T *) src, num_extra->neg, num_extra->pos);
    *dst = copy(encoded);
    *dst_size = encoded.size();
    return DATASTORE_SUCCESS;
}

template <typename T, typename = enable_if_t <std::is_floating_point<T>::value> >
int floating_point(void *src, const size_t,
                   void **dst, size_t *dst_size,
                   void *extra) {
    if (!src || !dst || !dst_size || !extra) {
        return DATASTORE_ERROR;
    }

    NumericExtra *num_extra = (NumericExtra *) extra;

    const std::string encoded = elen::encode::floating_point(* (T *) src,
                                                             std::is_same<float, T>::value?num_extra->float_precision:(std::is_same<double, T>::value?num_extra->double_precision:0),
                                                             num_extra->neg, num_extra->pos);
    *dst = copy(encoded);
    *dst_size = encoded.size();
    return DATASTORE_SUCCESS;
}

}

namespace decode {

template <typename T, typename = enable_if_t <std::is_integral<T>::value> >
int integers(void *src, const size_t src_size,
             void **dst, size_t *dst_size,
             void *extra) {
    if (!src || !dst || !extra) {
        return DATASTORE_ERROR;
    }

    NumericExtra *num_extra = (NumericExtra *) extra;

    if (dst_size) {
        *dst_size = sizeof(T);
    }

    *dst = (T *) alloc(sizeof(T));

    * (T *) *dst = elen::decode::integers<T>(std::string((const char *) src, src_size), nullptr,
                                             num_extra->neg, num_extra->pos);
    return DATASTORE_SUCCESS;
}

template <typename T, typename = enable_if_t <std::is_floating_point<T>::value> >
int floating_point(void *src, const size_t src_size,
                   void **dst, size_t *dst_size,
                   void *extra) {
    if (!src || !dst || !dst_size || !extra) {
        return DATASTORE_ERROR;
    }

    NumericExtra *num_extra = (NumericExtra *) extra;

    if (dst_size) {
        *dst_size = sizeof(T);
    }

    *dst = (T *) alloc(sizeof(T));

    * (T *) *dst = elen::decode::floating_point<T>(std::string((const char *) src, src_size),
                                                   num_extra->neg, num_extra->pos);
    return DATASTORE_SUCCESS;
}

}
}
}

#endif
