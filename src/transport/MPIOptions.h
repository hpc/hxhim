#ifndef HXHIM_TRANSPORT_INIT_MPI_TRANSPORT_OPTIONS
#define HXHIM_TRANSPORT_INIT_MPI_TRANSPORT_OPTIONS

#include <stdint.h>

#include <mpi.h>

#include "mdhim_constants.h"
#include "mdhim_options_struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

int mdhim_options_init_mpi_transport(mdhim_options_t *opts, const MPI_Comm comm, const size_t alloc_size, const size_t regions);

#ifdef __cplusplus
}
#endif

#endif
