#ifndef TRANSPORT_MPI_OPTIONS_HPP
#define TRANSPORT_MPI_OPTIONS_HPP

#include <cstdint>

#include <mpi.h>

#include "transport/constants.h"
#include "transport/options.hpp"

namespace Transport {
namespace MPI {

struct Options : ::Transport::Options {
    Options(MPI_Comm comm,
            const std::size_t alloc_size,
            const std::size_t regions,
            const std::size_t listeners)
        : ::Transport::Options(TRANSPORT_MPI),
        comm(comm),
        alloc_size(alloc_size),
        regions(regions),
        listeners(listeners)
    {}

    MPI_Comm comm;
    const std::size_t alloc_size;
    const std::size_t regions;
    const std::size_t listeners;
};

}
}

#endif
