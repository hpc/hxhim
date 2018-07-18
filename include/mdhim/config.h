#ifndef MDHIM_CONFIG
#define MDHIM_CONFIG

#include <mpi.h>

#include "options.h"

#ifdef __cplusplus
extern "C"
{
#endif

/** @description Use this function in place of mdhim_options_init when searching for configurations in multiple predefined places */
int mdhim_default_config_reader(mdhim_options_t *opts, const MPI_Comm comm);

#ifdef __cplusplus
}
#endif

#endif
