#ifndef HXHIM_TRANSPORT_MPI_ENDPOINT_BASE
#define HXHIM_TRANSPORT_MPI_ENDPOINT_BASE

#include <pthread.h>
#include <stdexcept>
#include <unistd.h>

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "mdhim_constants.h"
#include "transport.hpp"

/**
 * MPIEndpointBase
 * Base class containing common values and functions for MPI Transport related classes
 */
class MPIEndpointBase {
    public:
        MPIEndpointBase(const MPI_Comm comm, volatile int &shutdown);
        virtual ~MPIEndpointBase();

        MPI_Comm Comm() const;
        int Rank() const;
        int Size() const;

        void Flush(MPI_Request *req);

    protected:
        MPI_Comm comm_;
        static pthread_mutex_t mutex_;

        int rank_;
        int size_;

        volatile int &shutdown_;
};

#endif
