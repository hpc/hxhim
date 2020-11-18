#ifndef TRANSFORM_HPP
#define TRANSFORM_HPP

#include <map>

#include "datastore/transform.h"
#include "hxhim/constants.h"
#include "utils/type_traits.hpp"

namespace datastore {
    namespace Transform {
        /** Common values used by numeric encoding/decoding */
        struct NumericExtra {
            NumericExtra();

            char neg;                // used by integer and floating point transforms
            char pos;                // used by integer and floating point transforms
            size_t float_precision;  // used by float encoding
            size_t double_precision; // used by double encoding
        };

        struct Callbacks {
            /** All functions should alloc (NOT construct) memory for *dst */
            std::map<hxhim_data_t,
                     std::pair<hxhim_encode_func, void *> > encode;
            std::map<hxhim_data_t,
                     std::pair<hxhim_decode_func, void *> > decode;

            NumericExtra numeric_extra;
        };

        Callbacks *default_callbacks();

        /** All *dst must be dealloc-ed (NOT destruct-ed) by the caller of these functions */
        namespace encode {
            template <typename T, typename = enable_if_t <std::is_integral<T>::value> >
            int integers(void *src, const size_t,
                         void **dst, size_t *dst_size,
                         void *extra);

            template <typename T, typename = enable_if_t <std::is_floating_point<T>::value> >
            int floating_point(void *src, const size_t,
                               void **dst, size_t *dst_size,
                               void *extra);
        }

        namespace decode {
            template <typename T, typename = enable_if_t <std::is_integral<T>::value> >
            int integers(void *src, const size_t src_size,
                         void **dst, size_t *dst_size,
                         void *extra);

            template <typename T, typename = enable_if_t <std::is_floating_point<T>::value> >
            int floating_point(void *src, const size_t src_size,
                               void **dst, size_t *dst_size,
                               void *extra);
        }
    }
}

#include "datastore/transform.tpp"

#endif
