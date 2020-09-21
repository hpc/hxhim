#ifndef HXHIM_CONFIG_H
#define HXHIM_CONFIG_H

#include <mpi.h>

#include "options.h"
#include "struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

int hxhim_config_default_reader(hxhim_options_t *opts, MPI_Comm comm);

#ifdef __cplusplus
}
#endif

#endif
