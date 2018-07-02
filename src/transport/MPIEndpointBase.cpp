#include "transport/MPIEndpointBase.hpp"
#include "transport/MPIPacker.hpp"
#include "transport/MPIUnpacker.hpp"
#include "transport/MPIEndpoint.hpp"

pthread_mutex_t MPIEndpointBase::mutex_ = PTHREAD_MUTEX_INITIALIZER;

MPIEndpointBase::MPIEndpointBase(const MPI_Comm comm, FixedBufferPool *fbp)
    : comm_(comm),
      fbp_(fbp)
{
    if (comm_ == MPI_COMM_NULL) {
        throw std::runtime_error("Received MPI_COMM_NULL as communicator");
    }

    if (MPI_Comm_rank(comm_, &rank_) != MPI_SUCCESS) {
        throw std::runtime_error("Failed to get rank within MPI communicator");
    }

    if (MPI_Comm_size(comm_, &size_) != MPI_SUCCESS) {
        throw std::runtime_error("Failed to get the size of the MPI communicator");
    }

    if (!fbp_) {
        throw std::runtime_error("Received bad Memory Pool");
    }
}

MPIEndpointBase::~MPIEndpointBase() {}

MPI_Comm MPIEndpointBase::Comm() const {
    return comm_;
}

int MPIEndpointBase::Rank() const {
    return rank_;
}

int MPIEndpointBase::Size() const {
    return size_;
}
