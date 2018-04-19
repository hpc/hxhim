/*
 * MDHIM TNG
 *
 * MDHIM Fortran90 API implementation
 */

#include <stdlib.h>
#include <sys/time.h>
#include <stdio.h>
#include "mdhim.h"
#include "mdhim_fortran.h"
#include "range_server.h"
#include "local_client.h"
#include "partitioner.h"
#include "mdhim_options.h"
#include "indexes.h"
#include "mdhim_private.h"
#include "transport_private.hpp"

/*! \mainpage MDHIM TNG
 *
 * \section intro_sec Introduction
 *
 * MDHIM TNG is a key/value store for HPC
 *
 */


mdhim_t *md;
mdhim_options_t *opts = NULL;

/**
 * mdhimInit
 * Initializes MDHIM - Collective call
 *
 * @param appTransport  the communicator that was passed in from the application (e.g., MPI_COMM_WORLD)
 * @param opts Options structure for DB creation, such as name, and primary key type
 * @return mdhim_t* that contains info about this instance or NULL if there was an error
 */
void mdhimftinit(int *appTransport) {
  MPI_Comm comm;

  comm = MPI_Comm_f2c(*((int *) appTransport));
  mdhimInit(md, opts);

  return;
}

void mdhimftopts_init() {
  //opts = mdhim_options_init();
}

void mdhimftput(void *key, int *key_size, void *val, int *val_size) {
  mdhimPut(md, key, *key_size, val, *val_size, NULL, NULL);
}

void mdhimftget(void *key, int *key_size, void *val, int val_size) {
   printf("Key: %s\n", (char *)key);
   mdhim_getrm_t *grm = mdhimGet(md, md->p->primary_index, key, *key_size, TransportGetMessageOp::GET_EQ);
   if (!grm || !grm->grm || grm->grm->error) {
     printf("Error getting value for key: %p from MDHIM\n", key);
   } else {
     printf("Successfully got value from MDHIM\n");
     memcpy(val, grm->grm->value, grm->grm->value_len);
   }
}

void mdhimftoptions_dbpath (char *path) {
   mdhim_options_set_db_path(opts, path);
}

void mdhimftoptions_dbname (char *name) {
   mdhim_options_set_db_name(opts, name);
}

void mdhimftoptions_dbtype (int type) {
   mdhim_options_set_db_type(opts, type);
}

void mdhimftoptions_server_factor (int factor) {
   mdhim_options_set_server_factor(opts, factor);
}

void mdhimftoptions_max_recs_per_slice (int recs) {
   mdhim_options_set_max_recs_per_slice(opts, recs);
}

void mdhimftoptions_debug_level (int level) {
   mdhim_options_set_debug_level(opts, MLOG_CRIT);
}

/**
 * mdhimClose
 * Closes MDHIM - Collective call
 *
 * @param appTransport  the communicator that was passed in from the application (e.g., MPI_COMM_WORLD)
 * @param opts Options structure for DB creation, such as name, and primary key type
 * @return mdhim_t* that contains info about this instance or NULL if there was an error
 */
void mdhimftclose() {
  int ret;

  ret = mdhimClose(md);
  return;
}
