/*
 * DB usage options.
 * Location and name of DB, type of DataSotre primary key type,
 */

#ifndef      __OPTIONS_H
#define      __OPTIONS_H

#include <stdint.h>
#include <mpi.h>

#ifdef __cplusplus
extern "C"
{
#endif

#define MDHIM_DS_NONE 0
#define MDHIM_DS_LEVELDB 1

#define MDHIM_COMM_NONE 0
#define MDHIM_COMM_MPI 1

/* Append option */
#define MDHIM_DB_OVERWRITE 0
#define MDHIM_DB_APPEND 1

// Options for the database (used when opening a MDHIM dataStore)
/**
 * @brief Structure used to set MDHIM options before initialization
 */
typedef struct mdhim_options {
    MPI_Comm comm;

    int dstype;
    int commtype;

	//Directory location of DBs
	const char *db_path;

	//Multiple paths of DBs
	char **db_paths;
	//Number of paths in db_paths
	int num_paths;

	const char *manifest_path;

	//Name of each DB (will be modified by adding "_<RANK>" to create multiple
	// unique DB for each rank server.
	const char *db_name;

	//Different types of dataStores
	//LEVELDB=1 (from data_store.h)
	int db_type;

	//Primary key type
	//MDHIM_INT_KEY, MDHIM_LONG_INT_KEY, MDHIM_FLOAT_KEY, MDHIM_DOUBLE_KEY
	//MDHIM_STRING_KEY, MDHIM_BYTE_KEY
	//(from partitioner.h)
	int db_key_type;

	//Force the creation of a new DB (deleting any previous versions if present)
	int db_create_new;

	//Whether to append a value to an existing key or overwrite the value
	//MDHIM_DB_APPEND to append or MDHIM_DB_OVERWRITE (default)
	int db_value_append;

	//DEBUG level
	int debug_level;

    //Used to determine the number of range servers which is based in
    //if myrank % rserver_factor == 0, then myrank is a server.
    // This option is used to set range_server_factor previously a defined var.
    int rserver_factor;

    //Maximum size of a slice. A ranger server may server several slices.
    uint64_t max_recs_per_slice;

	//Number of worker threads per range server
	int num_wthreads;

	//Login Credentials
	const char *db_host;
	const char *dbs_host;
	const char *db_user;
	const char *db_upswd;
	const char *dbs_user;
	const char *dbs_upswd;
} mdhim_options_t;

int mdhim_options_init(mdhim_options_t* opts);
void mdhim_options_set_defaults(mdhim_options_t* opts);
void mdhim_options_set_db_path(mdhim_options_t* opts, const char *path);
void mdhim_options_set_db_paths(mdhim_options_t* opts, char **paths, int num_paths);
void mdhim_options_set_db_name(mdhim_options_t* opts, const char *name);
void mdhim_options_set_db_type(mdhim_options_t* opts, int type);
void mdhim_options_set_key_type(mdhim_options_t* opts, int key_type);
void mdhim_options_set_create_new_db(mdhim_options_t* opts, int create_new);
void mdhim_options_set_login_c(mdhim_options_t* opts, char* db_hl, char *db_ln, char *db_pw, char *dbs_hl, char *dbs_ln, char *dbs_pw);
void mdhim_options_set_debug_level(mdhim_options_t* opts, int dbug);
void mdhim_options_set_value_append(mdhim_options_t* opts, int append);
void mdhim_options_set_server_factor(mdhim_options_t* opts, int server_factor);
void mdhim_options_set_max_recs_per_slice(mdhim_options_t* opts, uint64_t max_recs_per_slice);
void mdhim_options_set_num_worker_threads(mdhim_options_t* opts, int num_wthreads);
void set_manifest_path(mdhim_options_t* opts, const char *path);
//void mdhim_options_destroy(mdhim_options_t *opts);
#ifdef __cplusplus
}
#endif

#endif
