#include "ThalliumOptions.h"
#include "ThalliumOptions.hpp"
#include "mdhim_options_private.h"

int mdhim_options_init_thallium_transport(mdhim_options_t* opts, const char *protocol) {
    if (!opts || !opts->p) {
        return MDHIM_ERROR;
    }

    delete opts->p->transport;
    return (opts->p->transport = new ThalliumOptions(protocol))?MDHIM_SUCCESS:MDHIM_ERROR;
}
