#if HXHIM_HAVE_THALLIUM

#ifndef TRANSPORT_THALLIUM_INIT_HPP
#define TRANSPORT_THALLIUM_INIT_HPP

#include "hxhim/struct.h"
#include "hxhim/options.h"

namespace Transport {
namespace Thallium {

int init(hxhim_t *hx, hxhim_options_t *opts);

}
}

#endif

#endif