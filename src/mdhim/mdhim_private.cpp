#include <cstdlib>

#include "client.h"
#include "indexes.h"
#include "local_client.h"
#include "mdhim_private.h"
#include "partitioner.h"

int mdhim_private_init(mdhim_private* mdp, int dstype, int commtype) {
    int rc = 0;
    if (dstype == MDHIM_DS_LEVELDB) {
        rc = MDHIM_SUCCESS;
    }
    else {
        mlog(MDHIM_CLIENT_CRIT, "Invalid data store type specified");
        rc = MDHIM_DB_ERROR;
        goto err_out;
    }

    if (commtype == MDHIM_COMM_MPI) {
        MPIEndpoint *ep = new MPIEndpoint(MPIInstance::instance().Comm());
        MPIEndpointGroup *eg = new MPIEndpointGroup(MPIInstance::instance().Comm());
        mdp->comm = new CommTransport(ep, eg);
        rc = MDHIM_SUCCESS;
        goto err_out;
    }

err_out:
    return rc;
}

struct mdhim_rm_t *_put_record(struct mdhim *md, struct index_t *index,
                               void *key, int key_len,
                               void *value, int value_len) {
	struct mdhim_rm_t *rm = NULL;
	rangesrv_list *rl, *rlp;
	struct index_t *lookup_index, *put_index;

	put_index = index;
	if (index->type == LOCAL_INDEX) {
		lookup_index = get_index(md, index->primary_id);
		if (!lookup_index) {
			return NULL;
		}
	} else {
		lookup_index = index;
	}

	//Get the range server this key will be sent to
	if (put_index->type == LOCAL_INDEX) {
		if ((rl = get_range_servers(md, lookup_index, value, value_len)) ==
		    NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
			     "Error while determining range server in mdhimBPut",
			     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
			return NULL;
		}
	} else {
		//Get the range server this key will be sent to
		if ((rl = get_range_servers(md, lookup_index, key, key_len)) == NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
			     "Error while determining range server in _put_record",
			     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
			return NULL;
		}
	}

	while (rl) {
        struct mdhim_putm_t *pm = (mdhim_putm_t*)malloc(sizeof(struct mdhim_putm_t));
		if (!pm) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
			     "Error while allocating memory in _put_record",
			     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
			return NULL;
		}

		//Initialize the put message
		pm->basem.mtype = MDHIM_PUT;
		pm->key = key;
		pm->key_len = key_len;
		pm->value = value;
		pm->value_len = value_len;
		pm->basem.server_rank = (int) *rl->ri->transport->Endpoint()->Address(); // TODO: change this to from server_rank to server_address
		pm->basem.index = put_index->id;
		pm->basem.index_type = put_index->type;

		//If I'm a range server and I'm the one this key goes to, send the message locally
		if (im_range_server(put_index) && (int) *md->p->comm->Endpoint()->Address() == pm->basem.server_rank) {
			rm = local_client_put(md, pm);
		} else {
			//Send the message through the network as this message is for another rank
			rm = client_put(md, pm);
			free(pm);
		}

		rlp = rl;
		rl = rl->next;
		free(rlp);
	}

	return rm;
}

/* Creates a linked list of mdhim_rm_t messages */
struct mdhim_brm_t *_create_brm(struct mdhim_rm_t *rm) {
	struct mdhim_brm_t *brm;

	if (!rm) {
		return NULL;
	}

	brm = (mdhim_brm_t*)malloc(sizeof(struct mdhim_brm_t));
	memset(brm, 0, sizeof(struct mdhim_brm_t));
	brm->error = rm->error;
	brm->basem.mtype = rm->basem.mtype;
	brm->basem.index = rm->basem.index;
	brm->basem.index_type = rm->basem.index_type;
	brm->basem.server_rank = rm->basem.server_rank;

	return brm;
}

/* adds new to the list pointed to by head */
void _concat_brm(struct mdhim_brm_t *head, struct mdhim_brm_t *addition) {
	struct mdhim_brm_t *brmp;

	brmp = head;
	while (brmp->next) {
		brmp = brmp->next;
	}

	brmp->next = addition;

	return;
}

struct mdhim_brm_t *_bput_records(struct mdhim *md, struct index_t *index,
				  void **keys, int *key_lens,
				  void **values, int *value_lens,
				  int num_keys) {
	struct mdhim_bputm_t **bpm_list, *lbpm;
	struct mdhim_bputm_t *bpm;
	struct mdhim_brm_t *brm, *brm_head;
	struct mdhim_rm_t *rm;
	int i;
	rangesrv_list *rl, *rlp;
	struct index_t *lookup_index, *put_index;

	put_index = index;
	if (index->type == LOCAL_INDEX) {
		lookup_index = get_index(md, index->primary_id);
		if (!lookup_index) {
			return NULL;
		}
	} else {
		lookup_index = index;
	}

	//Check to see that we were given a sane amount of records
	if (num_keys > MAX_BULK_OPS) {
		mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
		     "To many bulk operations requested in mdhimBGetOp",
		     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
		return NULL;
	}

	//The message to be sent to ourselves if necessary
	lbpm = NULL;
	//Create an array of bulk put messages that holds one bulk message per range server
	bpm_list = (mdhim_bputm_t**)malloc(sizeof(struct mdhim_bputm_t *) * lookup_index->num_rangesrvs);

	//Initialize the pointers of the list to null
	for (i = 0; i < lookup_index->num_rangesrvs; i++) {
		bpm_list[i] = NULL;
	}

	/* Go through each of the records to find the range server(s) the record belongs to.
	   If there is not a bulk message in the array for the range server the key belongs to,
	   then it is created.  Otherwise, the data is added to the existing message in the array.*/
	for (i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
		//Get the range server this key will be sent to
		if (put_index->type == LOCAL_INDEX) {
			if ((rl = get_range_servers(md, lookup_index, values[i], value_lens[i])) ==
			    NULL) {
				mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
				     "Error while determining range server in mdhimBPut",
				     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
				continue;
			}
		} else {
			if ((rl = get_range_servers(md, lookup_index, keys[i], key_lens[i])) ==
			    NULL) {
				mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
				     "Error while determining range server in mdhimBPut",
				     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
				continue;
			}
		}

		//There could be more than one range server returned in the case of the local index
		while (rl) {
			if (rl->ri->rank != (int) *md->p->comm->Endpoint()->Address()) {
				//Set the message in the list for this range server
				bpm = bpm_list[rl->ri->rangesrv_num - 1];
			} else {
				//Set the local message
				bpm = lbpm;
			}

			//If the message doesn't exist, create one
			if (!bpm) {
				bpm = (mdhim_bputm_t*)malloc(sizeof(struct mdhim_bputm_t));
				bpm->keys = (void**)malloc(sizeof(void *) * MAX_BULK_OPS);
				bpm->key_lens = (int*)malloc(sizeof(int) * MAX_BULK_OPS);
				bpm->values = (void**)malloc(sizeof(void *) * MAX_BULK_OPS);
				bpm->value_lens = (int*)malloc(sizeof(int) * MAX_BULK_OPS);
				bpm->num_keys = 0;
				bpm->basem.server_rank = rl->ri->rank;
				bpm->basem.mtype = MDHIM_BULK_PUT;
				bpm->basem.index = put_index->id;
				bpm->basem.index_type = put_index->type;
				if (rl->ri->rank != (int) *md->p->comm->Endpoint()->Address()) {
					bpm_list[rl->ri->rangesrv_num - 1] = bpm;
				} else {
					lbpm = bpm;
				}
			}

			//Add the key, lengths, and data to the message
			bpm->keys[bpm->num_keys] = keys[i];
			bpm->key_lens[bpm->num_keys] = key_lens[i];
			bpm->values[bpm->num_keys] = values[i];
			bpm->value_lens[bpm->num_keys] = value_lens[i];
			bpm->num_keys++;
			rlp = rl;
			rl = rl->next;
			free(rlp);
		}
	}

	//Make a list out of the received messages to return
	brm_head = client_bput(md, put_index, bpm_list);
	if (lbpm) {
		rm = local_client_bput(md, lbpm);
                if (rm) {
			brm = _create_brm(rm);
                        brm->next = brm_head;
                        brm_head = brm;
                        free(rm);
                }
	}

	//Free up messages sent
	for (i = 0; i < lookup_index->num_rangesrvs; i++) {
		if (!bpm_list[i]) {
			continue;
		}

		free(bpm_list[i]->keys);
		free(bpm_list[i]->values);
		free(bpm_list[i]->key_lens);
		free(bpm_list[i]->value_lens);
		free(bpm_list[i]);
	}

	free(bpm_list);

	//Return the head of the list
	return brm_head;
}

struct mdhim_bgetrm_t *_bget_records(struct mdhim *md, struct index_t *index,
                                     void **keys, int *key_lens,
                                     int num_keys, int num_records, int op) {
	//Create an array of bulk get messages that holds one bulk message per range server
	struct mdhim_bgetm_t **bgm_list = (mdhim_bgetm_t**)calloc(index->num_rangesrvs, sizeof(struct mdhim_bgetm_t *));
	struct mdhim_bgetm_t *lbgm = nullptr;	//The message to be sent to ourselves if necessary
	rangesrv_list *rl = NULL;

	/* Go through each of the records to find the range server the record belongs to.
	   If there is not a bulk message in the array for the range server the key belongs to,
	   then it is created.  Otherwise, the data is added to the existing message in the array.*/
	for (int i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
		//Get the range server this key will be sent to
		if ((op == MDHIM_GET_EQ || op == MDHIM_GET_PRIMARY_EQ) &&
		    index->type != LOCAL_INDEX &&
		    (rl = get_range_servers(md, index, keys[i], key_lens[i])) ==
		    NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
			     "Error while determining range server in mdhimBget",
			     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
			free(bgm_list);
			return NULL;
		} else if ((index->type == LOCAL_INDEX ||
			   (op != MDHIM_GET_EQ && op != MDHIM_GET_PRIMARY_EQ)) &&
			   (rl = get_range_servers_from_stats(md, index, keys[i], key_lens[i], op)) ==
			   NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
			     "Error while determining range server in mdhimBget",
			     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
			free(bgm_list);
			return NULL;
		}

		while (rl) {
            struct mdhim_bgetm_t *bgm = nullptr;
			if (rl->ri->rank != (int) *md->p->comm->Endpoint()->Address()) {
				//Set the message in the list for this range server
				bgm = bgm_list[rl->ri->rangesrv_num - 1];
			} else {
				//Set the local message
				bgm = lbgm;
			}

			//If the message doesn't exist, create one
			if (!bgm) {
				bgm = (mdhim_bgetm_t*)malloc(sizeof(struct mdhim_bgetm_t));
				//bgm->keys = malloc(sizeof(void *) * MAX_BULK_OPS);
				//bgm->key_lens = malloc(sizeof(int) * MAX_BULK_OPS);
				bgm->keys = (void**)malloc(sizeof(void *) * num_keys);
				bgm->key_lens = (int*)malloc(sizeof(int) * num_keys);
				bgm->num_keys = 0;
				bgm->num_recs = num_records;
				bgm->basem.server_rank = rl->ri->rank;
				bgm->basem.mtype = MDHIM_BULK_GET;
				bgm->op = (op == MDHIM_GET_PRIMARY_EQ) ? MDHIM_GET_EQ : op;
				bgm->basem.index = index->id;
				bgm->basem.index_type = index->type;
				if (rl->ri->rank != (int) *md->p->comm->Endpoint()->Address()) {
					bgm_list[rl->ri->rangesrv_num - 1] = bgm;
				} else {
					lbgm = bgm;
				}
			}

			//Add the key, lengths, and data to the message
			bgm->keys[bgm->num_keys] = keys[i];
			bgm->key_lens[bgm->num_keys] = key_lens[i];
			bgm->num_keys++;

			rangesrv_list *rlp = rl;
			rl = rl->next;
			free(rlp);
		}
	}

	//Make a list out of the received messages to return
	struct mdhim_bgetrm_t *bgrm_head = client_bget(md, index, bgm_list);
	if (lbgm) {
        struct mdhim_bgetrm_t *lbgrm = local_client_bget(md, lbgm);
		lbgrm->next = bgrm_head;
		bgrm_head = lbgrm;
	}

	for (int i = 0; i < index->num_rangesrvs; i++) {
		if (!bgm_list[i]) {
			continue;
		}

		free(bgm_list[i]->keys);
		free(bgm_list[i]->key_lens);
		free(bgm_list[i]);
	}

	free(bgm_list);

	return bgrm_head;
}

/**
 * Deletes multiple records from MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to delete
 * @param key_lens     array with lengths of each key in keys
 * @param num_keys  the number of keys to delete (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or NULL on error
 */
struct mdhim_brm_t *_bdel_records(struct mdhim *md, struct index_t *index,
				  void **keys, int *key_lens,
				  int num_keys) {
	struct mdhim_bdelm_t **bdm_list;
	struct mdhim_bdelm_t *bdm, *lbdm;
	struct mdhim_brm_t *brm, *brm_head;
	struct mdhim_rm_t *rm;
	int i;
	rangesrv_list *rl;

	//The message to be sent to ourselves if necessary
	lbdm = NULL;
	//Create an array of bulk del messages that holds one bulk message per range server
	bdm_list = (mdhim_bdelm_t**)malloc(sizeof(struct mdhim_bdelm_t *) * index->num_rangesrvs);
	//Initialize the pointers of the list to null
	for (i = 0; i < index->num_rangesrvs; i++) {
		bdm_list[i] = NULL;
	}

	/* Go through each of the records to find the range server the record belongs to.
	   If there is not a bulk message in the array for the range server the key belongs to,
	   then it is created.  Otherwise, the data is added to the existing message in the array.*/
	for (i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
		//Get the range server this key will be sent to
		if (index->type != LOCAL_INDEX &&
		    (rl = get_range_servers(md, index, keys[i], key_lens[i])) ==
		    NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
			     "Error while determining range server in mdhimBdel",
			     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
			continue;
		} else if (index->type == LOCAL_INDEX &&
			   (rl = get_range_servers_from_stats(md, index, keys[i],
							      key_lens[i], MDHIM_GET_EQ)) ==
			   NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
			     "Error while determining range server in mdhimBdel",
			     ((std::string) (*md->p->comm->Endpoint()->Address())).c_str());
			continue;
		}

		if (rl->ri->rank != (int) *md->p->comm->Endpoint()->Address()) {
			//Set the message in the list for this range server
			bdm = bdm_list[rl->ri->rangesrv_num - 1];
		} else {
			//Set the local message
			bdm = lbdm;
		}

		//If the message doesn't exist, create one
		if (!bdm) {
			bdm = (mdhim_bdelm_t*)malloc(sizeof(struct mdhim_bdelm_t));
			bdm->keys = (void**)malloc(sizeof(void *) * MAX_BULK_OPS);
			bdm->key_lens = (int*)malloc(sizeof(int) * MAX_BULK_OPS);
			bdm->num_keys = 0;
			bdm->basem.server_rank = rl->ri->rank;
			bdm->basem.mtype = MDHIM_BULK_DEL;
			bdm->basem.index = index->id;
			bdm->basem.index_type = index->type;
			if (rl->ri->rank != (int) *md->p->comm->Endpoint()->Address()) {
				bdm_list[rl->ri->rangesrv_num - 1] = bdm;
			} else {
				lbdm = bdm;
			}
		}

		//Add the key, lengths, and data to the message
		bdm->keys[bdm->num_keys] = keys[i];
		bdm->key_lens[bdm->num_keys] = key_lens[i];
		bdm->num_keys++;
	}

	//Make a list out of the received messages to return
	brm_head = client_bdelete(md, index, bdm_list);
	if (lbdm) {
		rm = local_client_bdelete(md, lbdm);
		brm = (mdhim_brm_t*)malloc(sizeof(struct mdhim_brm_t));
		brm->error = rm->error;
		brm->basem.mtype = rm->basem.mtype;
		brm->basem.index = rm->basem.index;
		brm->basem.index_type = rm->basem.index_type;
		brm->basem.server_rank = rm->basem.server_rank;
		brm->next = brm_head;
		brm_head = brm;
		free(rm);
	}

	for (i = 0; i < index->num_rangesrvs; i++) {
		if (!bdm_list[i]) {
			continue;
		}

		free(bdm_list[i]->keys);
		free(bdm_list[i]->key_lens);
		free(bdm_list[i]);
	}

	free(bdm_list);

	//Return the head of the list
	return brm_head;
}

int _which_server(struct mdhim *md, void *key, int key_len)
{
    rangesrv_list *rl;
    rl = get_range_servers(md, md->p->primary_index, key, key_len);
    /* what is the difference between 'rank' and 'rangeserv_num' ? */
    return (rl->ri->rank);
}
