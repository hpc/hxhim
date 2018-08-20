#include "hxhim/private.hpp"
#include "hxhim/utils.hpp"

namespace hxhim {

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
 * free_request
 * Frees a pointer from the request data FixedBufferPool
 *
 * @param hx the HXHIM session
 * @param ptr a pointer from the request data FixedBufferPool
 */
void free_request(hxhim_t *hx, void *ptr) {
    return GetRequestFBP(hx)->release(ptr);
}

/**
 * free_response
 * Frees a pointer from the response data FixedBufferPool
 *
 * @param hx the HXHIM session
 * @param ptr a pointer from the response data FixedBufferPool
 */
void free_response(hxhim_t *hx, void *ptr) {
    return GetResponseFBP(hx)->release(ptr);
}

}
