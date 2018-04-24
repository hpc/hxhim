#ifndef MDHIM_CONFIG
#define MDHIM_CONFIG

#include <mpi.h>

#include "mdhim_options.h"

#ifdef __cplusplus
extern "C"
{
#endif

/** @description Use this function in place of mdhim_options_init when searching for configuration files in multiple predefined places */
int mdhim_default_config_reader(mdhim_options_t *opts, const MPI_Comm comm);

#ifdef __cplusplus
}
#endif

#endif
