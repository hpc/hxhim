#include "MPIOptions.h"
#include "MPIOptions.hpp"
#include "mdhim_options_private.h"

int mdhim_options_init_mpi_transport(mdhim_options_t* opts, const MPI_Comm comm, const size_t alloc_size, const size_t regions) {
    if (!opts || !opts->p) {
        return MDHIM_ERROR;
    }

    delete opts->p->transport;
    return (opts->p->transport = new MPIOptions(comm, alloc_size, regions))?MDHIM_SUCCESS:MDHIM_ERROR;
}
