#ifndef TRANSPORT_THALLIUM_UTILITIES_HPP
#define TRANSPORT_THALLIUM_UTILITIES_HPP

#include <memory>
#include <unordered_map>

#include <mpi.h>
#include <thallium.hpp>

#include "transport/constants.hpp"

namespace Transport {
namespace Thallium {

typedef std::shared_ptr<thallium::engine> Engine_t;
typedef std::shared_ptr<thallium::remote_procedure> RPC_t;
typedef std::shared_ptr<thallium::endpoint> Endpoint_t;

/** @description Get all thallium lookup addresses using MPI as transport */
int get_addrs(const MPI_Comm comm, const thallium::engine &engine, std::unordered_map<int, std::string> &addrs);

}
}

#endif
