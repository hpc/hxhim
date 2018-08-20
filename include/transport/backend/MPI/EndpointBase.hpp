#ifndef TRANSPORT_MPI_ENDPOINT_BASE_HPP
#define TRANSPORT_MPI_ENDPOINT_BASE_HPP

#include <pthread.h>
#include <stdexcept>
#include <unistd.h>

#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"
#include <mpi.h>

#include "utils/FixedBufferPool.hpp"

namespace Transport {
namespace MPI {

/**
 * EndpointBase
 * Base class containing common values and functions for MPI Transport related classes
 */
class EndpointBase {
    public:
        EndpointBase(const MPI_Comm comm,
                     FixedBufferPool *packed,
                     FixedBufferPool *buffers);
        virtual ~EndpointBase();

        MPI_Comm Comm() const;
        int Rank() const;
        int Size() const;

    protected:
        static pthread_mutex_t mutex;
        MPI_Comm comm;
        int rank;
        int size;

        FixedBufferPool *packed;
        FixedBufferPool *buffers;

};

}
}

#endif
