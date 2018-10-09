#ifndef HXHIM_RANGE_SERVER_HPP
#define HXHIM_RANGE_SERVER_HPP

#include "hxhim/struct.h"
#include "transport/Messages/Messages.hpp"

namespace hxhim {
namespace range_server {

/** @description Utility function to determine whether or not a rank has a range server */
bool is_range_server(const int rank, const std::size_t client_ratio, const std::size_t server_ratio);

Transport::Response::Response *range_server(hxhim_t *hx, const Transport::Request::Request *req);

}
}

#endif
