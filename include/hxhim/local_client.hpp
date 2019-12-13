#ifndef HXHIM_LOCAL_CLIENT_HPP
#define HXHIM_LOCAL_CLIENT_HPP

#include "struct.h"
#include "transport/transport.hpp"

namespace hxhim {

Transport::Response::Response *local_client(hxhim_t *hx, const Transport::Request::Request *req);

}

#endif
