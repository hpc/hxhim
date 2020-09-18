#ifndef TRANSPORTS_HPP
#define TRANSPORTS_HPP

#include <set>

#include "hxhim/struct.h"
#include "transport/Messages/Messages.hpp"
#include "transport/Options.hpp"
#include "transport/backend/backends.hpp"
#include "transport/constants.hpp"
#include "transport/transport.hpp"

namespace Transport {

int init(hxhim_t *hx,
         const std::size_t client_ratio,
         const std::size_t server_ratio,
         const std::set<int> &endpointgroup,
         Options *opts);

int destroy(hxhim_t *hx);

}

#endif
