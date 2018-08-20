#include "transport/backend/MPI/EndpointBase.hpp"

namespace Transport {
namespace MPI {

pthread_mutex_t EndpointBase::mutex = PTHREAD_MUTEX_INITIALIZER;

EndpointBase::EndpointBase(const MPI_Comm comm,
                           FixedBufferPool *packed,
                           FixedBufferPool *buffers)
    : comm(comm),
      rank(-1),
      size(-1),
      packed(packed),
      buffers(buffers)
{
    if (comm == MPI_COMM_NULL) {
        throw std::runtime_error("Received MPI_COMM_NULL as communicator");
    }

    if (MPI_Comm_rank(comm, &rank) != MPI_SUCCESS) {
        throw std::runtime_error("Failed to get rank within MPI communicator");
    }

    if (MPI_Comm_size(comm, &size) != MPI_SUCCESS) {
        throw std::runtime_error("Failed to get the size of the MPI communicator");
    }

    if (!packed) {
        throw std::runtime_error("Received bad Memory Pool for packing data");
    }

    if (!buffers) {
        throw std::runtime_error("Received bad Memory Pool for storing data");
    }
}

EndpointBase::~EndpointBase() {}

MPI_Comm EndpointBase::Comm() const {
    return comm;
}

int EndpointBase::Rank() const {
    return rank;
}

int EndpointBase::Size() const {
    return size;
}

}
}
