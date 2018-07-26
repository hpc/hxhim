#ifndef HXHIM_FLUSH_FUNCTIONS_HPP
#define HXHIM_FLUSH_FUNCTIONS_HPP

#include "hxhim/struct.h"
#include "hxhim/Results.hpp"

namespace hxhim {

Results *FlushPuts(hxhim_t *hx);
Results *FlushGets(hxhim_t *hx);
Results *FlushGetOps(hxhim_t *hx);
Results *FlushDeletes(hxhim_t *hx);
Results *Flush(hxhim_t *hx);

}

#endif
