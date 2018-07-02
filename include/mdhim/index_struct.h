#ifndef INDEX_STRUCT_H
#define INDEX_STRUCT_H

#include <stddef.h>
#include <stdint.h>

#include "utils/uthash.h"
#include <mpi.h>

#include "range_server_info_struct.h"

#define BAD_INDEX       0
#define PRIMARY_INDEX   1
#define SECONDARY_INDEX 2
#define LOCAL_INDEX     3
#define REMOTE_INDEX    4

typedef struct mdhim_store mdhim_store_t;
typedef struct mdhim_stat mdhim_stat_t;

/*
 * Remote Index info
 * Contains information about a remote index
 *
 * A remote index means that an index can be served by one or more range servers
 */
typedef struct index {
    int id;         // Hash key

    //The abstracted data store layer that mdhim uses to store and retrieve records
    mdhim_store_t **mdhim_stores;

    //Options for the mdhim data store
    int key_type;             //The key type used in the db
    int db_type;              //The database type
    int type;                 /* The type of index (PRIMARY_INDEX, SECONDARY_INDEX, LOCAL_INDEX) */
    int primary_id;           /* The primary index id if this is a secondary index */
    rangesrv_info_t *rangesrvs_by_database; /* Hash table of the range servers
                                               serving this index. Key is database number */
    rangesrv_info_t *rangesrvs_by_num; /* Hash table of the range servers
                                          serving this index. Key is range server number */
    rangesrv_info_t *rangesrvs_by_rank; /* Hash table of the range servers
                                           serving this index. Key is the rank */
    //Used to determine the number of range servers which is based in
    //if myrank % RANGE_SERVER_FACTOR == 0, then myrank is a server
    int range_server_factor;

    //Maximum size of a slice. A range server may serve several slices.
    uint64_t slice_size;

    //This communicator is for range servers only to talk to each other
    MPI_Comm rs_comm;
    /* The rank of the range server master that will broadcast stat data to all clients
       This rank is the rank in comm not in the range server communicator */
    int rangesrv_master;

    //The number of range servers for this index
    size_t num_rangesrvs;
    size_t dbs_per_server;

    //The rank's range server information, if it is a range server for this index
    rangesrv_info_t myinfo;

    //Statistics retrieved from the mdhimStatFlush operation
    mdhim_stat_t *stats;

    size_t *histogram;

    UT_hash_handle hh;         /* makes this structure hashable */
} index_t;

#endif
