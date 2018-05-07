#ifndef CLONE_VOID
#define CLONE_VOID

#include <cstddef>
#include <cstring>

#include "mdhim_constants.h"

int _clone(void *src, std::size_t src_len, void **dst);
int _cleanup(void *data);

int _clone(std::size_t count, void **srcs, std::size_t *src_lens, void ***dsts, std::size_t **dst_lens);
int _cleanup(std::size_t count, void **data, std::size_t *len);

#endif
