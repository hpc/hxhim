#include "utils/reverse_bytes.h"

size_t reverse_bytes(void *src, const size_t src_len, void *dst) {
    if (!src || !dst) {
        return 0;
    }

    const size_t middle = src_len / 2;
    for(size_t i = 0; i < middle; i++) {
        ((char *) dst)[i] = ((char *) src)[src_len - i - 1];
    }

    return src_len;
}
