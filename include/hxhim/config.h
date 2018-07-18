#ifndef HXHIM_CONFIG
#define HXHIM_CONFIG

#include <mpi.h>

#include "options.h"
#include "struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

int hxhim_default_config_reader(hxhim_options_t *opts, MPI_Comm comm);

#ifdef __cplusplus
}
#endif

#endif
