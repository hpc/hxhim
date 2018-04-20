#ifndef INDEX_STRUCT_H
#define INDEX_STRUCT_H

#include <stdint.h>

#include "uthash.h"
#include <mpi.h>

#include "range_server_info_struct.h"

/*
 * Remote Index info
 * Contains information about a remote index
 *
 * A remote index means that an index can be served by one or more range servers
 */
typedef struct index {
	int id;         // Hash key
	char *name;     // Secondary Hash key

	//The abstracted data store layer that mdhim uses to store and retrieve records
	struct mdhim_store_t *mdhim_store;
	//Options for the mdhim data store
	int key_type;             //The key type used in the db
	int db_type;              //The database type
	int type;                 /* The type of index
				     (PRIMARY_INDEX, SECONDARY_INDEX, LOCAL_INDEX) */
	int primary_id;           /* The primary index id if this is a secondary index */
	rangesrv_info_t *rangesrvs_by_num; /* Hash table of the range servers
					    serving this index.  Key is range server number */
	rangesrv_info_t *rangesrvs_by_rank; /* Hash table of the range servers
					     serving this index.  Key is the rank */
        //Used to determine the number of range servers which is based in
        //if myrank % RANGE_SERVER_FACTOR == 0, then myrank is a server
	int range_server_factor;

        //Maximum size of a slice. A range server may serve several slices.
	uint64_t mdhim_max_recs_per_slice;

	//This communicator is for range servers only to talk to each other
	MPI_Comm rs_comm;
	/* The rank of the range server master that will broadcast stat data to all clients
	   This rank is the rank in mdhim_comm not in the range server communicator */
	int rangesrv_master;

	//The number of range servers for this index
	int32_t num_rangesrvs;

	//The rank's range server information, if it is a range server for this index
	rangesrv_info_t myinfo;

	//Statistics retrieved from the mdhimStatFlush operation
	struct mdhim_stat *stats;

	UT_hash_handle hh;         /* makes this structure hashable */
	UT_hash_handle hh_name;    /* makes this structure hashable by name */
} index_t;

#endif
