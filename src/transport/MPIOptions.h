#ifndef HXHIM_TRANSPORT_MPI_OPTIONS
#define HXHIM_TRANSPORT_MPI_OPTIONS

#include <stdint.h>
#include <mpi.h>

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct MPIOptions {
    MPI_Comm comm;
    size_t alloc_size;
    size_t regions;
} MPIOptions_t;

#ifdef __cplusplus
}
#endif

#endif
