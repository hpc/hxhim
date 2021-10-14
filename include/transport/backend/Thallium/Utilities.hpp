#ifndef TRANSPORT_THALLIUM_UTILITIES_HPP
#define TRANSPORT_THALLIUM_UTILITIES_HPP

#include <map>

#include <mpi.h>
#include <thallium.hpp>

#include "transport/constants.hpp"

namespace Transport {
namespace Thallium {

/** @description Get all thallium lookup addresses using MPI as transport */
int get_addrs(const MPI_Comm comm, const thallium::engine &engine, std::map<int, std::string> &addrs);

}
}

#endif
