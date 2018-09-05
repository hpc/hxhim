#ifndef HXHIM_RANGE_SERVER_HPP
#define HXHIM_RANGE_SERVER_HPP

#include "hxhim/struct.h"
#include "transport/Messages/Messages.hpp"

namespace hxhim {
namespace range_server {

Transport::Response::Response *range_server(hxhim_t *hx, const Transport::Request::Request *req);

}
}

#endif
