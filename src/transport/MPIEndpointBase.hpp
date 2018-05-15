#ifndef MDHIM_TRANSPORT_MPI_ENDPOINT_BASE_HPP
#define MDHIM_TRANSPORT_MPI_ENDPOINT_BASE_HPP

#include <pthread.h>
#include <stdexcept>
#include <unistd.h>

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "mdhim_constants.h"
#include "MemoryManagers.hpp"

/**
 * MPIEndpointBase
 * Base class containing common values and functions for MPI Transport related classes
 */
class MPIEndpointBase {
    public:
        MPIEndpointBase(const MPI_Comm comm, FixedBufferPool *fbp);
        virtual ~MPIEndpointBase();

        MPI_Comm Comm() const;
        int Rank() const;
        int Size() const;

    protected:
        MPI_Comm comm_;
        static pthread_mutex_t mutex_;

        int rank_;
        int size_;

        FixedBufferPool *fbp_;
};

#endif
