#ifndef MDHIM_OPTIONS_PRIVATE_H
#define MDHIM_OPTIONS_PRIVATE_H

#include <cstdint>
#include <set>

#include "mlog2.h"
#include "mlogfacs2.h"

#include "transport_options.hpp"

typedef struct mdhim_transport_options {
    TransportOptions *transport_specific;
    std::set<int> endpointgroup;
} mdhim_transport_options_t;

// Options for the database (used when opening a MDHIM dataStore)
typedef struct mdhim_db_options {
    //Directory location of DBs
    char *path;

    //Multiple paths of DBs
    char **paths;

    //Number of paths in db_paths
    int num_paths;

    char *manifest_path;

    //Name of each DB (will be modified by adding "_<RANK>" to create multiple
    // unique DB for each rank server.
    char *name;

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

    //Used to determine the number of range servers which is based in
    //if myrank % rserver_factor == 0, then myrank is a server.
    // This option is used to set range_server_factor previously a defined var.
    int rserver_factor;

    //Number of databases under a range server
    //Each rank holds databases with sequential IDs
    //    n = md->rank / rserver_factor
    //    [n * dbs_per_server, (n + 1) * dbs_per_server)
    int dbs_per_server;

    //Maximum size of a slice. A ranger server may server several slices.
    uint64_t max_recs_per_slice;

    //Number of worker threads per range server
    int num_wthreads;

    //Login Credentials
    char *db_host;
    char *dbs_host;
    char *db_user;
    char *db_upswd;
    char *dbs_user;
    char *dbs_upswd;
} mdhim_db_options_t;

/**
 * @brief Structure used to set MDHIM options before initialization
 */
typedef struct mdhim_options_private {
    mdhim_transport_options_t *transport;
    mdhim_db_options_t *db;
} mdhim_options_private_t;

#endif
