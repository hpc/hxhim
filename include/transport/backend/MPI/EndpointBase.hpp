#ifndef TRANSPORT_MPI_ENDPOINT_BASE_HPP
#define TRANSPORT_MPI_ENDPOINT_BASE_HPP

#include <memory>
#include <pthread.h>
#include <stdexcept>
#include <unistd.h>

#include <mpi.h>

namespace Transport {
namespace MPI {

/**
 * EndpointBase
 * Base class containing common values and functions for MPI Transport related classes
 */
class EndpointBase {
    public:
        EndpointBase(const MPI_Comm comm);
        virtual ~EndpointBase();

        MPI_Comm Comm() const;
        int Rank() const;
        int Size() const;

    protected:
        MPI_Comm comm;
        int rank;
        int size;
};

}
}

#endif
