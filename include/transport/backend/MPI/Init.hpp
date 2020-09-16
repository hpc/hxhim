#ifndef TRANSPORT_MPI_INIT_HPP
#define TRANSPORT_MPI_INIT_HPP

#include "hxhim/struct.h"
#include "hxhim/options.h"

namespace Transport {
namespace MPI {

Transport *init(hxhim_t *hx, hxhim_options_t *opts);

}
}

#endif
