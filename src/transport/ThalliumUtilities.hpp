#ifndef MDHIM_TRANSPORT_THALLIUM_UTILITIES_HPP
#define MDHIM_TRANSPORT_THALLIUM_UTILITIES_HPP

#include <map>
#include <memory>

#include <mpi.h>
#include <thallium.hpp>

#include "mdhim_constants.h"

namespace Thallium {

/** @description Get all thallium lookup addresses using MPI as transport */
int get_addrs(const MPI_Comm comm, const std::shared_ptr<thallium::engine> &engine, std::map<int, std::string> &addrs);

typedef std::shared_ptr<thallium::engine> Engine_t;
typedef std::shared_ptr<thallium::remote_procedure> RPC_t;
typedef std::shared_ptr<thallium::endpoint> Endpoint_t;

}

#endif
