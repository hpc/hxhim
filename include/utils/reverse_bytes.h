#ifndef HXHIM_REVERSE_BYTES
#define HXHIM_REVERSE_BYTES

#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

size_t reverse_bytes(void *src, const size_t src_len, void *dst);

#ifdef __cplusplus
}
#endif

#endif
