#include "hxhim/private.hpp"
#include "hxhim/utils.hpp"

namespace hxhim {

/**
 * GetPackedFBP
 * Returns the FixedPackedPool used to allocate packeds
 * This is a utility function only available in C++
 *
 * @param hx the HXHIM session
 * @return a pointer to the FixedBufferPool used to allocate packeds, or nullptr
 */
FixedBufferPool *GetPackedFBP(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return nullptr;
    }

    return hx->p->memory_pools.packed;
}

/**
 * GetBufferFBP
 * Returns the FixedBufferPool used to allocate buffers
 * This is a utility function only available in C++
 *
 * @param hx the HXHIM session
 * @return a pointer to the FixedBufferPool used to allocate buffers, or nullptr
 */
FixedBufferPool *GetBufferFBP(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return nullptr;
    }

    return hx->p->memory_pools.buffers;
}

/**
 * GetKeyFBP
 * Returns the FixedBufferPool used to allocate keys
 * This is a utility function only available in C++
 *
 * @param hx the HXHIM session
 * @return a pointer to the FixedBufferPool used to allocate keys, or nullptr
 */
FixedBufferPool *GetKeyFBP(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return nullptr;
    }

    return hx->p->memory_pools.keys;
}

/**
 * GetArrayFBP
 * Returns the FixedBufferPool used to allocate arrays
 * This is a utility function only available in C++
 *
 * @param hx the HXHIM session
 * @return a pointer to the FixedBufferPool used to allocate arrays, or nullptr
 */
FixedBufferPool *GetArrayFBP(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return nullptr;
    }

    return hx->p->memory_pools.arrays;
}

/**
 * GetRequestFBP
 * Returns the FixedBufferPool used to allocate requests
 * This is a utility function only available in C++
 *
 * @param hx the HXHIM session
 * @return a pointer to the FixedBufferPool used to allocate requests, or nullptr
 */
FixedBufferPool *GetRequestFBP(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return nullptr;
    }

    return hx->p->memory_pools.requests;
}

/**
 * GetResponseFBP
 * Returns the FixedBufferPool used to allocate responses
 * This is a utility function only available in C++
 *
 * @param hx the HXHIM session
 * @return a pointer to the FixedBufferPool used to allocate responses, or nullptr
 */
FixedBufferPool *GetResponseFBP(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return nullptr;
    }

    return hx->p->memory_pools.responses;
}

/**
 * GetResultFBP
 * Returns the FixedBufferPool used to allocate a result
 * This is a utility function only available in C++
 *
 * @param hx the HXHIM session
 * @return a pointer to the FixedBufferPool used to allocate results, or nullptr
 */
FixedBufferPool *GetResultFBP(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return nullptr;
    }

    return hx->p->memory_pools.result;
}

/**
 * free_request
 * Frees a pointer from the request data FixedBufferPool
 *
 * @param hx the HXHIM session
 * @param ptr a pointer from the request data FixedBufferPool
 */
void free_request(hxhim_t *hx, Transport::Request::Request *ptr) {
    return GetRequestFBP(hx)->release(ptr);
}

/**
 * free_response
 * Frees a pointer from the response data FixedBufferPool
 *
 * @param hx the HXHIM session
 * @param ptr a pointer from the response data FixedBufferPool
 */
void free_response(hxhim_t *hx, Transport::Response::Response *ptr) {
    return GetResponseFBP(hx)->release(ptr);
}

}
