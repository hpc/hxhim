#ifndef TRANSFORM_H
#define TRANSFORM_H

#ifdef __cplusplus
#include <cstddef>

extern "C"
{
#else
#include <stddef.h>
#endif

/**
 * Datastore format encoding callback signature for encoding data for placement into datastores
 *
 * @param src        A pointer to the source data
 * @param src_size   The size of the source data
 * @param dst        The address of the pointer to the decoded data
 * @param dst_size   The size of the transformed data
 * @param extra      Any extra arguments that the function will use
 * @param HXHIM_SUCCESS or HXHIM_ERROR
 */
typedef int (*hxhim_encode_func)(void *src, const size_t src_size,
                                 void **dst, size_t *dst_size,
                                 void *extra);

/**
 * Datastore format encoding callback signature for deencoding data obtained from datastores
 *
 * @param src        A pointer to the source data
 * @param src_size   The size of the source data
 * @param dst        The address of the pointer to the decoded data
 * @param dst_size   The size of the transformed data
 * @param extra      Any extra arguments that the function will use
 * @param HXHIM_SUCCESS or HXHIM_ERROR
 */
typedef int (*hxhim_decode_func)(void *src, const size_t src_size,
                                 void **dst, size_t *dst_size,
                                 void *extra);

#ifdef __cplusplus
}
#endif

#endif
