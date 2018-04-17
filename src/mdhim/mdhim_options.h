/*
 * DB usage options.
 * Location and name of DB, type of DataSotre primary key type,
 */

#ifndef      __OPTIONS_H
#define      __OPTIONS_H

#include <mpi.h>

#include "mdhim_constants.h"
#include "MPIOptions.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct mdhim_options_private mdhim_options_private_t;

typedef struct mdhim_options {
    mdhim_options_private_t *p;
} mdhim_options_t;

int mdhim_options_init(mdhim_options_t* opts);
void mdhim_options_set_transport(mdhim_options_t *opts, const int type, void *data);
void mdhim_options_set_db_path(mdhim_options_t* opts, const char *path);
void mdhim_options_set_db_paths(mdhim_options_t* opts, char **paths, int num_paths);
void mdhim_options_set_db_name(mdhim_options_t* opts, const char *name);
void mdhim_options_set_db_type(mdhim_options_t* opts, int type);
void mdhim_options_set_key_type(mdhim_options_t* opts, int key_type);
void mdhim_options_set_create_new_db(mdhim_options_t* opts, int create_new);
void mdhim_options_set_login_c(mdhim_options_t* opts, const char* db_hl, const char *db_ln, const char *db_pw, const char *dbs_hl, const char *dbs_ln, const char *dbs_pw);
void mdhim_options_set_debug_level(mdhim_options_t* opts, int dbug);
void mdhim_options_set_value_append(mdhim_options_t* opts, int append);
void mdhim_options_set_server_factor(mdhim_options_t* opts, int server_factor);
void mdhim_options_set_max_recs_per_slice(mdhim_options_t* opts, uint64_t max_recs_per_slice);
void mdhim_options_set_num_worker_threads(mdhim_options_t* opts, int num_wthreads);
void mdhim_options_set_manifest_path(mdhim_options_t* opts, const char *path);
int mdhim_options_destroy(mdhim_options_t *opts);

#ifdef __cplusplus
}
#endif

#endif
