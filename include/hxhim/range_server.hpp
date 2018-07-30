#ifndef HXHIM_RANGE_SERVER_HPP
#define HXHIM_RANGE_SERVER_HPP

#include "hxhim/struct.h"
#include "transport/Messages.hpp"

Transport::Response::Response *range_server(hxhim_t *hx, Transport::Request::Request *req);

#endif
