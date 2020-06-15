#ifndef TRANSPORT_MPI_OPTIONS_HPP
#define TRANSPORT_MPI_OPTIONS_HPP

#include <cstddef>

#include <mpi.h>

#include "transport/constants.hpp"
#include "transport/Options.hpp"

namespace Transport {
namespace MPI {

struct Options : ::Transport::Options {
    Options(MPI_Comm comm,
            const std::size_t listeners)
        : ::Transport::Options(TRANSPORT_MPI),
        comm(comm),
        listeners(listeners)
    {}

    const MPI_Comm comm;
    const std::size_t listeners;
};

}
}

#endif
