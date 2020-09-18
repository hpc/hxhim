#ifndef TRANSPORT_MPI_INIT_HPP
#define TRANSPORT_MPI_INIT_HPP

#include "hxhim/struct.h"
#include "Options.hpp"

namespace Transport {
namespace MPI {

Transport *init(hxhim_t *hx,
                const std::size_t client_ratio,
                const std::size_t server_ratio,
                const std::set<int> &endpointgroup,
                Options *opts);

}
}

#endif
