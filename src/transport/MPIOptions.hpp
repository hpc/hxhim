#ifndef HXHIM_TRANSPORT_MPI_OPTIONS
#define HXHIM_TRANSPORT_MPI_OPTIONS

#include <cstdint>

#include <mpi.h>

#include "transport_options.hpp"

class MPIOptions : virtual public TransportOptions {
    public:
        MPIOptions(MPI_Comm comm,
                   std::size_t alloc_size,
                   std::size_t regions)
            : TransportOptions(MDHIM_TRANSPORT_MPI),
              comm_(comm),
              alloc_size_(alloc_size),
              regions_(regions)
        {}

        MPI_Comm comm_;
        std::size_t alloc_size_;
        std::size_t regions_;
};

#endif
