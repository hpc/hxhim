#ifndef      __OPTIONS_H
#define      __OPTIONS_H

#include <mpi.h>

#include "mdhim_constants.h"
#include "mdhim_options_struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

int mdhim_options_init(mdhim_options_t *opts, const MPI_Comm comm, const int set_transport_defaults, const int set_db_defaults);

// Transport Options
int mdhim_options_init_transport(mdhim_options_t *opts, const int set_defaults);
int mdhim_options_set_mpi(mdhim_options_t *opts, const MPI_Comm comm, const size_t alloc_size, const size_t regions);
int mdhim_options_set_thallium(mdhim_options_t *opts, const char *protocol);
int mdhim_options_set_endpoint_group(mdhim_options_t *opts, const int *ranks, const int count);
int mdhim_options_add_endpoint_to_group(mdhim_options_t *opts, const int rank);
int mdhim_options_clear_endpoint_group(mdhim_options_t *opts);

// Database Options
int mdhim_options_init_db(mdhim_options_t *opts, const int set_defaults);
int mdhim_options_set_db_path(mdhim_options_t *opts, const char *path);
int mdhim_options_set_db_paths(mdhim_options_t *opts, char **paths, const int num_paths);
int mdhim_options_set_db_name(mdhim_options_t *opts, const char *name);
int mdhim_options_set_db_type(mdhim_options_t *opts, const int type);
int mdhim_options_set_key_type(mdhim_options_t *opts, const int key_type);
int mdhim_options_set_create_new_db(mdhim_options_t *opts, const int create_new);
int mdhim_options_set_db_host(mdhim_options_t *opts, const char* db_hl);
int mdhim_options_set_db_login(mdhim_options_t *opts, const char *db_ln);
int mdhim_options_set_db_password(mdhim_options_t *opts, const char *db_pw);
int mdhim_options_set_dbs_host(mdhim_options_t *opts, const char* dbs_hl);
int mdhim_options_set_dbs_login(mdhim_options_t *opts, const char *dbs_ln);
int mdhim_options_set_dbs_password(mdhim_options_t *opts, const char *dbs_pw);
int mdhim_options_set_login_c(mdhim_options_t *opts, const char* db_hl, const char *db_ln, const char *db_pw, const char *dbs_hl, const char *dbs_ln, const char *dbs_pw);
int mdhim_options_set_debug_level(mdhim_options_t *opts, const int dbug);
int mdhim_options_set_value_append(mdhim_options_t *opts, const int append);
int mdhim_options_set_server_factor(mdhim_options_t *opts, const int server_factor);
int mdhim_options_set_dbs_per_server(mdhim_options_t *opts, const int dbs_per_server);
int mdhim_options_set_max_recs_per_slice(mdhim_options_t *opts, const uint64_t max_recs_per_slice);
int mdhim_options_set_num_worker_threads(mdhim_options_t *opts, const int num_wthreads);
int mdhim_options_set_manifest_path(mdhim_options_t *opts, const char *path);

int mdhim_options_destroy(mdhim_options_t *opts);

#ifdef __cplusplus
}
#endif

#endif
