/*
 * DB usage options.
 * Location and name of DB, type of DataSotre primary key type,
 */

#ifndef MDHIM_OPTIONS_PRIVATE_H
#define MDHIM_OPTIONS_PRIVATE_H

#include <stdint.h>

#include "mlog2.h"
#include "mlogfacs2.h"

// Options for the underlying transport
typedef struct mdhim_transport_options {
    int type;
    void *data;
} mdhim_transport_options_t;

// Options for the database (used when opening a MDHIM dataStore)
typedef struct mdhim_db_options {
    //Directory location of DBs
    const char *path;

    //Multiple paths of DBs
    char **paths;

    //Number of paths in db_paths
    int num_paths;

    const char *manifest_path;

    //Name of each DB (will be modified by adding "_<RANK>" to create multiple
    // unique DB for each rank server.
    const char *name;

    //Different types of dataStores
    //LEVELDB=1 (from data_store.h)
    int type;

    //Primary key type
    //MDHIM_INT_KEY, MDHIM_LONG_INT_KEY, MDHIM_FLOAT_KEY, MDHIM_DOUBLE_KEY
    //MDHIM_STRING_KEY, MDHIM_BYTE_KEY
    //(from partitioner.h)
    int key_type;

    //Force the creation of a new DB (deleting any previous versions if present)
    int create_new;

    //Whether to append a value to an existing key or overwrite the value
    //MDHIM_DB_APPEND to append or MDHIM_DB_OVERWRITE (default)
    int value_append;

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
} mdhim_db_options_t;

/**
 * @brief Structure used to set MDHIM options before initialization
 */
typedef struct mdhim_options_private {
    mdhim_transport_options_t *transport;
    mdhim_db_options_t *db;
} mdhim_options_private_t;

#endif
