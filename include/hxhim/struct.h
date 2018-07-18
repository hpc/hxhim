#ifndef HXHIM_STRUCT
#define HXHIM_STRUCT

#include "bootstrap.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_private hxhim_private_t;

typedef struct {
    bootstrap_t mpi;

    hxhim_private_t *p;
} hxhim_t;

#ifdef __cplusplus
}
#endif

#endif
