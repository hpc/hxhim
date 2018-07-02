#ifndef MDHIM_TRANSPORT_MPI_OPTIONS_HPP
#define MDHIM_TRANSPORT_MPI_OPTIONS_HPP

#include <cstdint>

#include <mpi.h>

#include "transport_options.hpp"

class MPIOptions : virtual public TransportOptions {
    public:
        MPIOptions(MPI_Comm comm,
                   const std::size_t alloc_size,
                   const std::size_t regions,
                   const std::size_t listeners)
            : TransportOptions(MDHIM_TRANSPORT_MPI),
              comm_(comm),
              alloc_size_(alloc_size),
              regions_(regions),
              listeners_(listeners)
        {}

        MPI_Comm comm_;
        std::size_t alloc_size_;
        std::size_t regions_;
        std::size_t listeners_;
};

#endif
