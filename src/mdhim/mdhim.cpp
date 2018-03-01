/*
 * MDHIM TNG
 *
 * MDHIM API implementation
 */

#include <memory>
#include <stdlib.h>
#include <sys/time.h>
#include <stdio.h>
#include "mdhim.h"
#include "range_server.h"
#include "client.h"
#include "local_client.h"
#include "partitioner.h"
#include "mdhim_options.h"
#include "indexes.h"
#include "mdhim_private.h"

/*! \mainpage MDHIM TNG
 *
 * \section intro_sec Introduction
 *
 * MDHIM TNG is a key/value store for HPC
 *
 */

/**
 * groupInitialization
 * Initializes MPI values in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int groupInitialization(mdhim_t *md) {
    if (!md){
        return MDHIM_ERROR;
    }

    // TODO: remove this block//////////////////////
    //Don't allow MPI_COMM_NULL to be the communicator
    if (md->p->mdhim_comm == MPI_COMM_NULL) {
        return MDHIM_ERROR;
    }

	//Dup the communicator passed in for barriers between clients
	if (MPI_Comm_dup(md->p->mdhim_comm, &md->p->mdhim_client_comm) != MPI_SUCCESS) {
        return MDHIM_ERROR;
	}

	//Get the size of the main MDHIM communicator
	if (MPI_Comm_size(md->p->mdhim_comm, &md->p->mdhim_comm_size) != MPI_SUCCESS) {
        return MDHIM_ERROR;
	}
    // /////////////////////////////////////////////

    return MDHIM_SUCCESS;
}

/**
 * groupDestruction
 * Cleans up MPI values in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int groupDestruction(mdhim_t *md) {
    if (!md){
        return MDHIM_ERROR;
    }

    //Destroy the client communicator
	MPI_Comm_free(&md->p->mdhim_client_comm);

    return MDHIM_SUCCESS;
}

/**
 * indexnitialization
 * Initializes index values in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int indexInitialization(mdhim_t *md) {
    if (!md){
        return MDHIM_ERROR;
    }

	//Initialize the indexes and create the primary index
	md->p->indexes = NULL;
	md->p->indexes_by_name = NULL;
	if (pthread_rwlock_init(&md->p->indexes_lock, NULL) != 0) {
		return MDHIM_ERROR;
	}

	//Create the default remote primary index
	md->p->primary_index = create_global_index(md, md->p->db_opts->rserver_factor, md->p->db_opts->max_recs_per_slice,
                                               md->p->db_opts->db_type, md->p->db_opts->db_key_type, NULL);

	if (!md->p->primary_index) {
		return MDHIM_ERROR;
	}

    return MDHIM_SUCCESS;
}

/**
 * indexDestruction
 * Cleans up index values in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int indexDestruction(mdhim_t *md) {
    if (!md){
        return MDHIM_ERROR;
    }

    if (pthread_rwlock_destroy(&md->p->indexes_lock) != 0) {
        return MDHIM_ERROR;
    }

	indexes_release(md);

    return MDHIM_SUCCESS;
}

/**
 * mdhimInit
 * Initializes MDHIM
 *
 * @param md MDHIM context
 * @param opts Options structure for DB creation, such as name, and primary key type
 * @return MDHIM status value
 */
int mdhimInit(mdhim_t* md, mdhim_options_t *opts) {
    if (!md){
      return MDHIM_ERROR;
    }

    if (!opts){
      return MDHIM_ERROR;
    }

	//Open mlog - stolen from plfs
    //Assume opts has been initialized
	mlog_open((char *)"mdhim", 0, opts->debug_level, opts->debug_level, NULL, 0, MLOG_LOGPID, 0);

    //Initialize context variables based on options

    md->p = new mdhim_private;
    mdhim_private_init(md->p, MDHIM_DS_LEVELDB, MDHIM_COMM_MPI);

    md->p->mdhim_comm = opts->comm;
    md->p->mdhim_comm_lock = PTHREAD_MUTEX_INITIALIZER;

    //Required for index initialization
    md->p->mdhim_rs = NULL;
    md->p->db_opts = opts;

    //Initialize group members of context
    if (groupInitialization(md) != MDHIM_SUCCESS){
		mlog(MDHIM_CLIENT_CRIT, "MDHIM - Error Group Initialization Failed");
        return MDHIM_ERROR;
    }

	//Initialize the partitioner
	partitioner_init();

    //Initialize index members of context
    if (indexInitialization(md) != MDHIM_SUCCESS){
		mlog(MDHIM_CLIENT_CRIT, "MDHIM - Error Index Initialization Failed");
        return MDHIM_ERROR;
    }

    //Flag that won't be used until shutdown
    md->p->shutdown = 0;

    md->p->receive_msg_mutex = PTHREAD_MUTEX_INITIALIZER;
    md->p->receive_msg_ready_cv = PTHREAD_COND_INITIALIZER;
    md->p->receive_msg = NULL;

	return MDHIM_SUCCESS;
}

/**
 * Quits the MDHIM instance
 *
 * @param md MDHIM context to be closed
 * @return MDHIM status value
 */
int mdhimClose(struct mdhim *md) {
    if (!md || !md->p) {
        return MDHIM_ERROR;
    }

    //Force shutdown if not already started
    md->p->shutdown = 1;

	//Stop range server if I'm a range server
	if (md->p->mdhim_rs && (range_server_stop(md) != MDHIM_SUCCESS)) {
		return MDHIM_ERROR;
	}

	//Free up memory used by the partitioner
	partitioner_release();

	//Free up memory used by indexes
    if (indexDestruction(md) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

	//Free up memory used by message buffer
    free(md->p->receive_msg);

    //Clean up group members
    if (groupDestruction(md) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    delete md->p->comm;
    delete md->p;
    md->p = NULL;

	//Close MLog
	mlog_close();

	return MDHIM_SUCCESS;
}

/**
 * Commits outstanding MDHIM writes - collective call
 *
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimCommit(struct mdhim *md, struct index_t *index) {
	int ret = MDHIM_SUCCESS;
	struct mdhim_basem_t *cm;
	struct mdhim_rm_t *rm = NULL;

    if (!index) {
      index = md->p->primary_index;
    }

	MPI_Barrier(md->p->mdhim_client_comm);
	//If I'm a range server, send a commit message to myself
	if (im_range_server(index)) {
		cm = (mdhim_basem_t*) malloc(sizeof(struct mdhim_basem_t));
		cm->mtype = MDHIM_COMMIT;
		cm->index = index->id;
		cm->index_type = index->type;
		rm = local_client_commit(md, cm);
		if (!rm || rm->error) {
			ret = MDHIM_ERROR;
			mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - "
			     "Error while committing database in mdhimCommit",
			     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
		}

		if (rm) {
			free(rm);
		}
	}

	MPI_Barrier(md->p->mdhim_client_comm);

	return ret;
}

/**
 * Inserts a single record into MDHIM
 *
 * @param md main MDHIM context
 * @param primary_key        pointer to key to store
 * @param primary_key_len    the length of the key
 * @param value              pointer to the value to store
 * @param value_len          the length of the value
 * @param secondary_info     secondary global and local information for
                             inserting secondary global and local keys
 * @return                   mdhim_brm_t * or NULL on error
 */
struct mdhim_brm_t *mdhimPut(struct mdhim *md,
			     void *primary_key, int primary_key_len,
			     void *value, int value_len,
			     struct secondary_info *secondary_global_info,
			     struct secondary_info *secondary_local_info) {
	//Return message from each _put_record casll
	struct mdhim_brm_t *brm = NULL;

	if (!primary_key || !primary_key_len ||
	    !value || !value_len) {
		return NULL;
	}

	struct mdhim_rm_t *rm = _put_record(md, md->p->primary_index, primary_key, primary_key_len, value, value_len);
	if (!rm || rm->error) {
		return NULL;
	}

	struct mdhim_brm_t *head = _create_brm(rm);
	mdhim_full_release_msg(rm);

	//Insert the secondary local key if it was given
	if (secondary_local_info && secondary_local_info->secondary_index &&
	    secondary_local_info->secondary_keys &&
	    secondary_local_info->secondary_key_lens &&
	    secondary_local_info->num_keys) {
		void **primary_keys = (void**)malloc(sizeof(void *) * secondary_local_info->num_keys);
		int *primary_key_lens = (int*)malloc(sizeof(int) * secondary_local_info->num_keys);
		for (int i = 0; i < secondary_local_info->num_keys; i++) {
			primary_keys[i] = primary_key;
			primary_key_lens[i] = primary_key_len;
		}

		brm = _bput_records(md, secondary_local_info->secondary_index,
				    secondary_local_info->secondary_keys,
				    secondary_local_info->secondary_key_lens,
				    primary_keys, primary_key_lens,
				    secondary_local_info->num_keys);

		free(primary_keys);
		free(primary_key_lens);
		if (!brm) {
			return head;
		}

		_concat_brm(head, brm);
	}

	//Insert the secondary global key if it was given
	if (secondary_global_info && secondary_global_info->secondary_index &&
	    secondary_global_info->secondary_keys &&
	    secondary_global_info->secondary_key_lens &&
	    secondary_global_info->num_keys) {
		void **primary_keys = (void**)malloc(sizeof(void *) * secondary_global_info->num_keys);
		int *primary_key_lens = (int*)malloc(sizeof(int) * secondary_global_info->num_keys);
		for (int i = 0; i < secondary_global_info->num_keys; i++) {
			primary_keys[i] = primary_key;
			primary_key_lens[i] = primary_key_len;
		}
		brm = _bput_records(md, secondary_global_info->secondary_index,
				    secondary_global_info->secondary_keys,
				    secondary_global_info->secondary_key_lens,
				    primary_keys, primary_key_lens,
				    secondary_global_info->num_keys);

		free(primary_keys);
		free(primary_key_lens);
		if (!brm) {
		  return head;
		}

		_concat_brm(head, brm);
	}

	return head;
}

/**
 * Inserts a single record into an MDHIM secondary index
 *
 * @param md main MDHIM struct
 * @param secondary_key       pointer to key to store
 * @param secondary_key_len   the length of the key
 * @param primary_key     pointer to the primary_key
 * @param primary_key_len the length of the value
 * @return mdhim_brm_t * or NULL on error
 */
struct mdhim_brm_t *mdhimPutSecondary(struct mdhim *md,
				      struct index_t *secondary_index,
				      /*Secondary key */
				      void *secondary_key, int secondary_key_len,
				      /* Primary key */
				      void *primary_key, int primary_key_len) {

	//Return message list
	struct mdhim_brm_t *head;

	//Return message from each _put_record casll
	struct mdhim_rm_t *rm;

	rm = NULL;
	head = NULL;
	if (!secondary_key || !secondary_key_len ||
	    !primary_key || !primary_key_len) {
		return NULL;
	}

	rm = _put_record(md, secondary_index, secondary_key, secondary_key_len,
			 primary_key, primary_key_len);
	if (!rm || rm->error) {
		return head;
	}

	head = _create_brm(rm);
	mdhim_full_release_msg(rm);

	return head;
}

struct mdhim_brm_t *_bput_secondary_keys_from_info(struct mdhim *md,
						   struct secondary_bulk_info *secondary_info,
						   void **primary_keys, int *primary_key_lens,
						   int num_records) {
	int i, j;
	void **primary_keys_to_send;
	int *primary_key_lens_to_send;
	struct mdhim_brm_t *head, *newone;

	head = newone = NULL;
	for (i = 0; i < num_records; i++) {
		primary_keys_to_send =
                (void**)malloc(secondary_info->num_keys[i] * sizeof(void *));
		primary_key_lens_to_send =
                (int*)malloc(secondary_info->num_keys[i] * sizeof(int));

		for (j = 0; j < secondary_info->num_keys[i]; j++) {
			primary_keys_to_send[j] = primary_keys[i];
			primary_key_lens_to_send[j] = primary_key_lens[i];
		}

		newone = _bput_records(md, secondary_info->secondary_index,
				    secondary_info->secondary_keys[i],
				    secondary_info->secondary_key_lens[i],
				    primary_keys_to_send, primary_key_lens_to_send,
				    secondary_info->num_keys[i]);
		if (!head) {
			head = newone;
		} else if (newone) {
			_concat_brm(head, newone);
		}

		free(primary_keys_to_send);
		free(primary_key_lens_to_send);
	}

	return head;
}

/**
 * Inserts multiple records into MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to store
 * @param key_lens     array with lengths of each key in keys
 * @param values       pointer to array of values to store
 * @param value_lens   array with lengths of each value
 * @param num_records  the number of records to store (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or NULL on error
 */
struct mdhim_brm_t *mdhimBPut(struct mdhim *md,
			      void **primary_keys, int *primary_key_lens,
			      void **primary_values, int *primary_value_lens,
			      int num_records,
			      struct secondary_bulk_info *secondary_global_info,
			      struct secondary_bulk_info *secondary_local_info) {
	struct mdhim_brm_t *head, *newone;

	head = newone = NULL;
	if (!primary_keys || !primary_key_lens ||
	    !primary_values || !primary_value_lens) {
		return NULL;
	}

	head = _bput_records(md, md->p->primary_index, primary_keys, primary_key_lens,
			     primary_values, primary_value_lens, num_records);
	if (!head || head->error) {
		return head;
	}

	//Insert the secondary local keys if they were given
	if (secondary_local_info && secondary_local_info->secondary_index &&
	    secondary_local_info->secondary_keys &&
	    secondary_local_info->secondary_key_lens) {
		newone = _bput_secondary_keys_from_info(md, secondary_local_info, primary_keys,
						     primary_key_lens, num_records);
		if (newone) {
			_concat_brm(head, newone);
		}
	}

	//Insert the secondary global keys if they were given
	if (secondary_global_info && secondary_global_info->secondary_index &&
	    secondary_global_info->secondary_keys &&
	    secondary_global_info->secondary_key_lens) {
		newone = _bput_secondary_keys_from_info(md, secondary_global_info, primary_keys,
						     primary_key_lens, num_records);
		if (newone) {
			_concat_brm(head, newone);
		}
	}

	return head;
}

/**
 * Inserts multiple records into an MDHIM secondary index
 *
 * @param md           main MDHIM struct
 * @param index        the secondary index to use
 * @param keys         pointer to array of keys to store
 * @param key_lens     array with lengths of each key in keys
 * @param values       pointer to array of values to store
 * @param value_lens   array with lengths of each value
 * @param num_records  the number of records to store (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or NULL on error
 */
struct mdhim_brm_t *mdhimBPutSecondary(struct mdhim *md, struct index_t *secondary_index,
				       void **secondary_keys, int *secondary_key_lens,
				       void **primary_keys, int *primary_key_lens,
				       int num_records) {
	struct mdhim_brm_t *head, *newone;

	head = newone = NULL;
	if (!secondary_keys || !secondary_key_lens ||
	    !primary_keys || !primary_key_lens) {
		return NULL;
	}

	head = _bput_records(md, secondary_index, secondary_keys, secondary_key_lens,
			     primary_keys, primary_key_lens, num_records);
	if (!head || head->error) {
		return head;
	}

	return head;
}

/**
 * Retrieves a single record from MDHIM
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to get value of or last key to start from if op is
 (MDHIM_GET_NEXT or MDHIM_GET_PREV)
 * @param key_len   the length of the key
 * @param op        the operation type
 * @return mdhim_getrm_t * or NULL on error
 */
struct mdhim_bgetrm_t *mdhimGet(mdhim_t *md, struct index_t *index,
				void *key, int key_len,
				int op) {

	void **keys;
	int *key_lens;
	struct mdhim_bgetrm_t *bgrm_head;

	if (op != MDHIM_GET_EQ && op != MDHIM_GET_PRIMARY_EQ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
		     "Invalid op specified for mdhimGet",
		     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
		return NULL;
	}

	if (!index) {
		index = md->p->primary_index;
	}

	//Create an a array with the single key and key len passed in
	keys = (void**)malloc(sizeof(void *));
	key_lens = (int*)malloc(sizeof(int));
	keys[0] = key;
	key_lens[0] = key_len;

	//Get the linked list of return messages from mdhimBGet
	bgrm_head = _bget_records(md, index, keys, key_lens, 1, 1, op);

	//Clean up
	free(keys);
	free(key_lens);

	return bgrm_head;
}

/**
 * Retrieves multiple records from MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to get values for
 * @param key_lens     array with lengths of each key in keys
 * @param num_records  the number of keys to get (i.e., the number of keys in keys array)
 * @return mdhim_bgetrm_t * or NULL on error
 */
struct mdhim_bgetrm_t *mdhimBGet(mdhim_t *md, struct index_t *index,
				 void **keys, int *key_lens,
				 int num_keys, int op) {
	struct mdhim_bgetrm_t *bgrm_head, *lbgrm;
	void **primary_keys;
	int *primary_key_lens, plen;
	struct index_t *primary_index;
	int i;

	if (op != MDHIM_GET_EQ && op != MDHIM_GET_PRIMARY_EQ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
		     "Invalid operation for mdhimBGet",
		     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
		return NULL;
	}

	//Check to see that we were given a sane amount of records
	if (num_keys > MAX_BULK_OPS) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
		     "Too many bulk operations requested in mdhimBGet",
		     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
		return NULL;
	}

	if (!index) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
		     "Invalid index specified",
		     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
		return NULL;
	}

	bgrm_head = _bget_records(md, index, keys, key_lens, num_keys, 1, op);
	if (!bgrm_head) {
		return NULL;
	}

	if (op == MDHIM_GET_PRIMARY_EQ) {
		//Get the number of keys/values we received
		plen = 0;
		while (bgrm_head) {
			for (i = 0; i < bgrm_head->num_keys; i++) {
				plen++;
			}

			bgrm_head = bgrm_head->next;
		}

		if (plen > MAX_BULK_OPS) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
			     "Too many bulk operations would be performed "
			     "with the MDHIM_GET_PRIMARY_EQ operation.  Limiting "
			     "request to : %u key/values",
			     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str(), MAX_BULK_OPS);
			plen = MAX_BULK_OPS - 1;
		}

		primary_keys = (void**)malloc(sizeof(void *) * plen);
		primary_key_lens = (int*)malloc(sizeof(int) * plen);
		//Initialize the primary keys array and key lens array
		memset(primary_keys, 0, sizeof(void *) * plen);
		memset(primary_key_lens, 0, sizeof(int) * plen);

		//Get the primary keys from the previously received messages' values
		plen = 0;
		while (bgrm_head) {
			for (i = 0; i < bgrm_head->num_keys && plen < MAX_BULK_OPS ; i++) {
				primary_keys[plen] = malloc(bgrm_head->value_lens[i]);
				memcpy(primary_keys[plen], bgrm_head->values[i],
				       bgrm_head->value_lens[i]);
				primary_key_lens[plen] = bgrm_head->value_lens[i];
				plen++;
			}

			lbgrm = bgrm_head->next;
			mdhim_full_release_msg(bgrm_head);
			bgrm_head = lbgrm;
		}

		primary_index = get_index(md, index->primary_id);
		//Get the primary keys' values
		bgrm_head = _bget_records(md, primary_index,
					  primary_keys, primary_key_lens,
					  plen, 1, MDHIM_GET_EQ);

		//Free up the primary keys and lens arrays
		for (i = 0; i < plen; i++) {
			free(primary_keys[i]);
		}

		free(primary_keys);
		free(primary_key_lens);
	}

	//Return the head of the list
	return bgrm_head;
}


/**
 * Retrieves multiple sequential records from a single range server if they exist
 *
 * If the operation passed in is MDHIM_GET_NEXT or MDHIM_GET_PREV, this return all the records
 * starting from the key passed in in the direction specified
 *
 * If the operation passed in is MDHIM_GET_FIRST and MDHIM_GET_LAST and the key is NULL,
 * then this operation will return the keys starting from the first or last key
 *
 * If the operation passed in is MDHIM_GET_FIRST and MDHIM_GET_LAST and the key is not NULL,
 * then this operation will return the keys starting the first key on
 * the range server that the key resolves to
 *
 * @param md           main MDHIM struct
 * @param key          pointer to the key to start getting next entries from
 * @param key_len      the length of the key
 * @param num_records  the number of successive keys to get
 * @param op           the operation to perform (i.e., MDHIM_GET_NEXT or MDHIM_GET_PREV)
 * @return mdhim_bgetrm_t * or NULL on error
 */
struct mdhim_bgetrm_t *mdhimBGetOp(mdhim_t *md, struct index_t *index,
				   void *key, int key_len,
				   int num_records, int op) {
	void **keys;
	int *key_lens;
	struct mdhim_bgetrm_t *bgrm_head;

	if (num_records > MAX_BULK_OPS) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
		     "To many bulk operations requested in mdhimBGetOp",
		     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
		return NULL;
	}

	if (op == MDHIM_GET_EQ || op == MDHIM_GET_PRIMARY_EQ) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
		     "Invalid op specified for mdhimGet",
		     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
		return NULL;
	}

	//Create an a array with the single key and key len passed in
	keys = (void**)malloc(sizeof(void *));
	key_lens = (int*)malloc(sizeof(int));
	keys[0] = key;
	key_lens[0] = key_len;

	//Get the linked list of return messages from mdhimBGet
	bgrm_head = _bget_records(md, index, keys, key_lens, 1, num_records, op);

	//Clean up
	free(keys);
	free(key_lens);

	return bgrm_head;
}

/**
 * Deletes a single record from MDHIM
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to delete
 * @param key_len   the length of the key
 * @return mdhim_rm_t * or NULL on error
 */
struct mdhim_brm_t *mdhimDelete(mdhim_t *md, struct index_t *index,
				void *key, int key_len) {
	struct mdhim_brm_t *brm_head;
	void **keys;
	int *key_lens;

	keys = (void**)malloc(sizeof(void *));
	key_lens = (int*)malloc(sizeof(int));
	keys[0] = key;
	key_lens[0] = key_len;

	brm_head = _bdel_records(md, index, keys, key_lens, 1);

	free(keys);
	free(key_lens);

	return brm_head;
}

/**
 * Deletes multiple records from MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to delete
 * @param key_lens     array with lengths of each key in keys
 * @param num_records  the number of keys to delete (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or NULL on error
 */
struct mdhim_brm_t *mdhimBDelete(mdhim_t *md, struct index_t *index,
				 void **keys, int *key_lens,
				 int num_records) {
	struct mdhim_brm_t *brm_head;

	//Check to see that we were given a sane amount of records
	if (num_records > MAX_BULK_OPS) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
		     "To many bulk operations requested in mdhimBGetOp",
		     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
		return NULL;
	}

	brm_head = _bdel_records(md, index, keys, key_lens, num_records);

	//Return the head of the list
	return brm_head;
}

/**
 * Retrieves statistics from all the range servers - collective call
 *
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimStatFlush(mdhim_t *md, struct index_t *index) {
	int ret;

	MPI_Barrier(md->p->mdhim_client_comm);
	if ((ret = get_stat_flush(md, index)) != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
		     "Error while getting MDHIM stat data in mdhimStatFlush",
		     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
	}
	MPI_Barrier(md->p->mdhim_client_comm);

	return ret;
}

/**
 * Sets the secondary_info structure used in mdhimPut
 *
 */
struct secondary_info *mdhimCreateSecondaryInfo(struct index_t *secondary_index,
						void **secondary_keys, int *secondary_key_lens,
						int num_keys, int info_type) {
	struct secondary_info *sinfo;


	if (!secondary_index || !secondary_keys ||
	    !secondary_key_lens || !num_keys) {
		return NULL;
	}

	if (info_type != SECONDARY_GLOBAL_INFO &&
	    info_type != SECONDARY_LOCAL_INFO) {
		return NULL;
	}

	//Initialize the struct
	sinfo = (struct secondary_info*)malloc(sizeof(struct secondary_info));
	memset(sinfo, 0, sizeof(struct secondary_info));

	//Set the index fields
	sinfo->secondary_index = secondary_index;
	sinfo->secondary_keys = secondary_keys;
	sinfo->secondary_key_lens = secondary_key_lens;
	sinfo->num_keys = num_keys;
	sinfo->info_type = info_type;

	return sinfo;
}

void mdhimReleaseSecondaryInfo(struct secondary_info *si) {
	free(si);

	return;
}

/**
 * Sets the secondary_info structure used in mdhimBPut
 *
 */
struct secondary_bulk_info *mdhimCreateSecondaryBulkInfo(struct index_t *secondary_index,
							 void ***secondary_keys,
							 int **secondary_key_lens,
							 int *num_keys, int info_type) {

	struct secondary_bulk_info *sinfo;

	if (!secondary_index || !secondary_keys ||
	    !secondary_key_lens || !num_keys) {
		return NULL;
	}

	if (info_type != SECONDARY_GLOBAL_INFO &&
	    info_type != SECONDARY_LOCAL_INFO) {
		return NULL;
	}

	//Initialize the struct
	sinfo = (struct secondary_bulk_info*)malloc(sizeof(struct secondary_bulk_info));
	memset(sinfo, 0, sizeof(struct secondary_bulk_info));

	//Set the index fields
	sinfo->secondary_index = secondary_index;
	sinfo->secondary_keys = secondary_keys;
	sinfo->secondary_key_lens = secondary_key_lens;
	sinfo->num_keys = num_keys;
	sinfo->info_type = info_type;

	return sinfo;
}

void mdhimReleaseSecondaryBulkInfo(struct secondary_bulk_info *si) {
	free(si);

	return;
}
