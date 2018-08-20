#ifndef TRANSPORT_MPI_OPTIONS_HPP
#define TRANSPORT_MPI_OPTIONS_HPP

#include <cstddef>

#include <mpi.h>

#include "transport/constants.h"
#include "transport/options.hpp"

namespace Transport {
namespace MPI {

struct Options : ::Transport::Options {
    Options(MPI_Comm comm,
            const std::size_t listeners)
        : ::Transport::Options(TRANSPORT_MPI),
        comm(comm),
        listeners(listeners)
    {}

    MPI_Comm comm;
    const std::size_t listeners;
};

}
}

#endif
