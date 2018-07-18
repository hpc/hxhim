#ifndef HXHIM_BOOTSTRAP_HPP
#define HXHIM_BOOTSTRAP_HPP

#include <mpi.h>

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct {
    MPI_Comm comm;
    int rank;
    int size;
} bootstrap_t;

#ifdef __cplusplus
}
#endif

#endif
