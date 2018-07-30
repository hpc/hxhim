#include "transport/backend/MPI/EndpointBase.hpp"
// #include "transport/backend/MPI/Packer.hpp"
// #include "transport/backend/MPI/Unpacker.hpp"
#include "transport/backend/MPI/Endpoint.hpp"

namespace Transport {
namespace MPI {

pthread_mutex_t EndpointBase::mutex_ = PTHREAD_MUTEX_INITIALIZER;

EndpointBase::EndpointBase(const MPI_Comm comm, FixedBufferPool *fbp)
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

EndpointBase::~EndpointBase() {}

MPI_Comm EndpointBase::Comm() const {
    return comm_;
}

int EndpointBase::Rank() const {
    return rank_;
}

int EndpointBase::Size() const {
    return size_;
}

}
}
