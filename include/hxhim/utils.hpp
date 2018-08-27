#ifndef HXHIM_UTILS_HPP
#define HXHIM_UTILS_HPP

#include <type_traits>

#include "hxhim/struct.h"
#include "transport/Messages/Bulk.hpp"
#include "transport/Messages/Request.hpp"
#include "transport/Messages/Response.hpp"
#include "transport/Messages/Single.hpp"
#include "utils/FixedBufferPool.hpp"

namespace hxhim {

FixedBufferPool *GetPackedFBP(hxhim_t *hx);
FixedBufferPool *GetBufferFBP(hxhim_t *hx);
FixedBufferPool *GetKeyFBP(hxhim_t *hx);
FixedBufferPool *GetArrayFBP(hxhim_t *hx);
FixedBufferPool *GetRequestFBP(hxhim_t *hx);
FixedBufferPool *GetResponseFBP(hxhim_t *hx);

template <typename T, typename = std::enable_if_t<std::is_base_of<Transport::Request::Request, T>::value &&
                                                  std::is_base_of<Transport::Single, T>::value> >
T *alloc_request(hxhim_t *hx, const std::size_t count) {
    return hxhim::GetRequestFBP(hx)->acquire<T>(hxhim::GetBufferFBP(hx), count);
}

template <typename T, typename = std::enable_if_t<std::is_base_of<Transport::Request::Request, T>::value &&
                                                  std::is_base_of<Transport::Bulk, T>::value > >
T *alloc_brequest(hxhim_t *hx, const std::size_t count) {
    return hxhim::GetRequestFBP(hx)->acquire<T>(hxhim::GetArrayFBP(hx), hxhim::GetBufferFBP(hx), count);
}

void free_request(hxhim_t *hx, void *ptr);

template <typename T, typename = std::enable_if_t<std::is_base_of<Transport::Response::Response, T>::value &&
                                                  std::is_base_of<Transport::Single, T>::value> >
T *alloc_response(hxhim_t *hx, const std::size_t count) {
    return hxhim::GetResponseFBP(hx)->acquire<T>(hxhim::GetBufferFBP(hx), count);
}

template <typename T, typename = std::enable_if_t<std::is_base_of<Transport::Response::Response, T>::value &&
                                                  std::is_base_of<Transport::Bulk, T>::value > >
T *alloc_bresponse(hxhim_t *hx, const std::size_t count) {
    return hxhim::GetResponseFBP(hx)->acquire<T>(hxhim::GetArrayFBP(hx), hxhim::GetBufferFBP(hx), count);
}

void free_response(hxhim_t *hx, void *ptr);

}

#endif
