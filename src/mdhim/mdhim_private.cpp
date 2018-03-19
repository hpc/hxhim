#include <cstdlib>

#include "client.h"
#include "indexes.h"
#include "local_client.h"
#include "mdhim_private.h"
#include "partitioner.h"
#include "transport_mpi.hpp"

int mdhim_private_init(mdhim_private* mdp, int dstype, int commtype) {
    int rc = MDHIM_ERROR;
    if (!mdp) {
        goto err_out;
    }

    if (dstype == MDHIM_DS_LEVELDB) {
        rc = MDHIM_SUCCESS;
    }
    else {
        mlog(MDHIM_CLIENT_CRIT, "Invalid data store type specified");
        rc = MDHIM_DB_ERROR;
        goto err_out;
    }

    if (commtype == MDHIM_COMM_MPI) {
        MPIEndpoint *ep = new MPIEndpoint(MPIInstance::instance().Comm(), mdp->shutdown);
        MPIEndpointGroup *eg = new MPIEndpointGroup(MPIInstance::instance().Comm(), mdp->shutdown);
        mdp->transport = new Transport(ep, eg);
        mdp->work_receiver = MPIEndpointBase::listen_for_client;
        mdp->response_sender = MPIEndpointBase::respond_to_client;
        rc = MDHIM_SUCCESS;
        goto err_out;
    }

err_out:
    return rc;
}

TransportRecvMessage *_put_record(mdhim_t *md, index_t *index,
                                  void *key, int key_len,
                                  void *value, int value_len) {
	rangesrv_list *rl;
	index_t *lookup_index, *put_index;

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
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %s - "
			     "Error while determining range server in mdhimBPut",
			     ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
			return NULL;
		}
	} else {
		//Get the range server this key will be sent to
		if ((rl = get_range_servers(md, lookup_index, key, key_len)) == NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %s - "
			     "Error while determining range server in _put_record",
			     ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
			return NULL;
		}
	}

    TransportRecvMessage *rm = nullptr;
	while (rl) {
        TransportPutMessage *pm = new TransportPutMessage();
		if (!pm) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %s - "
			     "Error while allocating memory in _put_record",
			     ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
			return NULL;
		}

		//Initialize the put message
		pm->mtype = TransportMessageType::PUT;
		pm->key = key;
		pm->key_len = key_len;
		pm->value = value;
		pm->value_len = value_len;
		pm->server_rank = rl->ri->rank;
		pm->index = put_index->id;
		pm->index_type = put_index->type;

		//If I'm a range server and I'm the one this key goes to, send the message locally
		if (im_range_server(put_index) && (int) (*md->p->transport->Endpoint()->Address()) == pm->server_rank) {
			rm = local_client_put(md, pm);
		} else {
			//Send the message through the network as this message is for another rank
			rm = client_put(md, pm);
			free(pm);
		}

		rangesrv_list *rlp = rl;
		rl = rl->next;
		free(rlp);
	}

	return rm;
}

/* Creates a linked list of mdhim_rm_t messages */
TransportBRecvMessage *_create_brm(TransportRecvMessage *rm) {
    if (!rm) {
        return nullptr;
    }

    TransportBRecvMessage *brm = new TransportBRecvMessage();
    brm->error = rm->error;
    brm->mtype = rm->mtype;
    brm->index = rm->index;
    brm->index_type = rm->index_type;
    brm->server_rank = rm->server_rank;

    return brm;
}

/* adds new to the list pointed to by head */
void _concat_brm(TransportBRecvMessage *head, TransportBRecvMessage *addition) {
    TransportBRecvMessage *brmp = head;
    while (brmp->next) {
        brmp = brmp->next;
    }

    brmp->next = addition;
}

TransportBRecvMessage *_bput_records(mdhim_t *md, index_t *index,
                                     void **keys, int *key_lens,
                                     void **values, int *value_lens,
                                     int num_keys) {
    rangesrv_list *rl, *rlp;
    index_t *lookup_index = nullptr;

    index_t *put_index = index;
    if (index->type == LOCAL_INDEX) {
        lookup_index = get_index(md, index->primary_id);
        if (!lookup_index) {
            return nullptr;
        }
    } else {
        lookup_index = index;
    }

    //Check to see that we were given a sane amount of records
    if (num_keys > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
             "To many bulk operations requested in mdhimBGetOp",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        return nullptr;
    }

    //The message to be sent to ourselves if necessary
    TransportBPutMessage *lbpm = nullptr;

    //Create an array of bulk put messages that holds one bulk message per range server
    TransportBPutMessage **bpm_list = new TransportBPutMessage*[lookup_index->num_rangesrvs]();

    /* Go through each of the records to find the range server(s) the record belongs to.
       If there is not a bulk message in the array for the range server the key belongs to,
       then it is created.  Otherwise, the data is added to the existing message in the array.*/
    for (int i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
        //Get the range server this key will be sent to
        if (put_index->type == LOCAL_INDEX) {
            if ((rl = get_range_servers(md, lookup_index, values[i], value_lens[i])) ==
                nullptr) {
                mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
                     "Error while determining range server in mdhimBPut",
                     ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
                continue;
            }
        } else {
            if ((rl = get_range_servers(md, lookup_index, keys[i], key_lens[i])) ==
                nullptr) {
                mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
                     "Error while determining range server in mdhimBPut",
                     ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
                continue;
            }
        }

        //There could be more than one range server returned in the case of the local index
        while (rl) {
            TransportBPutMessage *bpm = nullptr;
            if (rl->ri->rank != (int) *md->p->transport->Endpoint()->Address()) {
                //Set the message in the list for this range server
                bpm = bpm_list[rl->ri->rangesrv_num - 1];
            } else {
                //Set the local message
                bpm = lbpm;
            }

            //If the message doesn't exist, create one
            if (!bpm) {
                bpm = new TransportBPutMessage();
                bpm->keys = (void**)malloc(sizeof(void *) * MAX_BULK_OPS);
                bpm->key_lens = (int*)malloc(sizeof(int) * MAX_BULK_OPS);
                bpm->values = (void**)malloc(sizeof(void *) * MAX_BULK_OPS);
                bpm->value_lens = (int*)malloc(sizeof(int) * MAX_BULK_OPS);
                bpm->num_keys = 0;
                bpm->server_rank = rl->ri->rank;
                bpm->mtype = TransportMessageType::BPUT;
                bpm->index = put_index->id;
                bpm->index_type = put_index->type;
                if (rl->ri->rank != (int) *md->p->transport->Endpoint()->Address()) {
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
    TransportBRecvMessage *brm_head = client_bput(md, put_index, bpm_list);
    if (lbpm) {
        TransportRecvMessage *rm = local_client_bput(md, lbpm);
        if (rm) {
            TransportBRecvMessage *brm = _create_brm(rm);
            brm->next = brm_head;
            brm_head = brm;
            free(rm);
        }
    }

    //Free up messages sent
    for (int i = 0; i < lookup_index->num_rangesrvs; i++) {
        delete bpm_list[i];
    }

    delete [] bpm_list;

    //Return the head of the list
    return brm_head;
}

TransportBGetRecvMessage *_bget_records(mdhim_t *md, index_t *index,
                                        void **keys, int *key_lens,
                                        int num_keys, int num_records,
                                        TransportGetMessageOp op) {
    if (!md || !index || !keys || !key_lens || !num_keys) {
        return nullptr;
    }

    //Create an array of bulk get messages that holds one bulk message per range server
    TransportBGetMessage **bgm_list = new TransportBGetMessage*[index->num_rangesrvs]();
    TransportBGetMessage *lbgm = nullptr; //The message to be sent to ourselves if necessary
    rangesrv_list *rl = nullptr;

    /* Go through each of the records to find the range server the record belongs to.
       If there is not a bulk message in the array for the range server the key belongs to,
       then it is created.  Otherwise, the data is added to the existing message in the array.*/
    for (int i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
        //Get the range server this key will be sent to
        if ((op == TransportGetMessageOp::GET_EQ || op == TransportGetMessageOp::GET_PRIMARY_EQ) &&
            index->type != LOCAL_INDEX &&
            (rl = get_range_servers(md, index, keys[i], key_lens[i])) ==
            nullptr) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
                 "Error while determining range server in mdhimBget",
                 ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
            delete [] bgm_list;
            return nullptr;
        } else if ((index->type == LOCAL_INDEX ||
                    (op != TransportGetMessageOp::GET_EQ && op != TransportGetMessageOp::GET_PRIMARY_EQ)) &&
                   (rl = get_range_servers_from_stats(md, index, keys[i], key_lens[i], op)) ==
                   nullptr) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
                 "Error while determining range server in mdhimBget",
                 ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
            delete [] bgm_list;
            return nullptr;
        }

        while (rl) {
            TransportBGetMessage *bgm = nullptr;
            if (rl->ri->rank != (int) *md->p->transport->Endpoint()->Address()) {
                //Set the message in the list for this range server
                bgm = bgm_list[rl->ri->rangesrv_num - 1];
            } else {
                bgm = lbgm;
            }

            //If the message doesn't exist, create one
            if (!bgm) {
                bgm = new TransportBGetMessage();
                bgm->keys = (void**)malloc(sizeof(void *) * num_keys);
                bgm->key_lens = (int*)malloc(sizeof(int) * num_keys);
                bgm->num_keys = 0;
                bgm->num_recs = num_records;
                bgm->server_rank = rl->ri->rank;
                bgm->mtype = TransportMessageType::BGET;
                bgm->op = (op == TransportGetMessageOp::GET_PRIMARY_EQ)?TransportGetMessageOp::GET_EQ:op;
                bgm->index = index->id;
                bgm->index_type = index->type;
                if (rl->ri->rank != (int) *md->p->transport->Endpoint()->Address()) {
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
    TransportBGetRecvMessage *bgrm_head = client_bget(md, index, bgm_list);
    if (lbgm) {
        TransportBGetRecvMessage *lbgrm = local_client_bget(md, lbgm);
        lbgrm->next = bgrm_head;
        bgrm_head = lbgrm;
    }

    delete [] bgm_list;

    return bgrm_head;
}

// /**
//  * Deletes multiple records from MDHIM
//  *
//  * @param md main MDHIM struct
//  * @param keys         pointer to array of keys to delete
//  * @param key_lens     array with lengths of each key in keys
//  * @param num_keys  the number of keys to delete (i.e., the number of keys in keys array)
//  * @return mdhim_brm_t * or nullptr on error
//  */
// mdhim_brm_t *_bdel_records(mdhim_t *md, index_t *index,
//                            void **keys, int *key_lens,
//                            int num_keys) {
//     struct mdhim_bdelm_t **bdm_list;
//     struct mdhim_bdelm_t *bdm, *lbdm;
//     struct mdhim_brm_t *brm, *brm_head;
//     struct mdhim_rm_t *rm;
//     int i;
//     rangesrv_list *rl;

//     //The message to be sent to ourselves if necessary
//     lbdm = nullptr;
//     //Create an array of bulk del messages that holds one bulk message per range server
//     bdm_list = (mdhim_bdelm_t**)malloc(sizeof(struct mdhim_bdelm_t *) * index->num_rangesrvs);
//     //Initialize the pointers of the list to null
//     for (i = 0; i < index->num_rangesrvs; i++) {
//         bdm_list[i] = nullptr;
//     }

//     /* Go through each of the records to find the range server the record belongs to.
//        If there is not a bulk message in the array for the range server the key belongs to,
//        then it is created.  Otherwise, the data is added to the existing message in the array.*/
//     for (i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
//         //Get the range server this key will be sent to
//         if (index->type != LOCAL_INDEX &&
//             (rl = get_range_servers(md, index, keys[i], key_lens[i])) ==
//             nullptr) {
//             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
//                  "Error while determining range server in mdhimBdel",
//                  ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
//             continue;
//         } else if (index->type == LOCAL_INDEX &&
//                (rl = get_range_servers_from_stats(md, index, keys[i],
//                                   key_lens[i], TransportGetMessageOp::EQ)) ==
//                nullptr) {
//             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
//                  "Error while determining range server in mdhimBdel",
//                  ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
//             continue;
//         }

//         if (rl->ri->rank != (int) *md->p->transport->Endpoint()->Address()) {
//             //Set the message in the list for this range server
//             bdm = bdm_list[rl->ri->rangesrv_num - 1];
//         } else {
//             //Set the local message
//             bdm = lbdm;
//         }

//         //If the message doesn't exist, create one
//         if (!bdm) {
//             bdm = (mdhim_bdelm_t*)malloc(sizeof(struct mdhim_bdelm_t));
//             bdm->keys = (void**)malloc(sizeof(void *) * MAX_BULK_OPS);
//             bdm->key_lens = (int*)malloc(sizeof(int) * MAX_BULK_OPS);
//             bdm->num_keys = 0;
//             bdm->basem.server_rank = rl->ri->rank;
//             bdm->basem.mtype = MDHIM_BULK_DEL;
//             bdm->basem.index = index->id;
//             bdm->basem.index_type = index->type;
//             if (rl->ri->rank != (int) *md->p->transport->Endpoint()->Address()) {
//                 bdm_list[rl->ri->rangesrv_num - 1] = bdm;
//             } else {
//                 lbdm = bdm;
//             }
//         }

//         //Add the key, lengths, and data to the message
//         bdm->keys[bdm->num_keys] = keys[i];
//         bdm->key_lens[bdm->num_keys] = key_lens[i];
//         bdm->num_keys++;
//     }

//     //Make a list out of the received messages to return
//     brm_head = client_bdelete(md, index, bdm_list);
//     if (lbdm) {
//         rm = local_client_bdelete(md, lbdm);
//         brm = (mdhim_brm_t*)malloc(sizeof(struct mdhim_brm_t));
//         brm->error = rm->error;
//         brm->basem.mtype = rm->basem.mtype;
//         brm->basem.index = rm->basem.index;
//         brm->basem.index_type = rm->basem.index_type;
//         brm->basem.server_rank = rm->basem.server_rank;
//         brm->next = brm_head;
//         brm_head = brm;
//         free(rm);
//     }

//     for (i = 0; i < index->num_rangesrvs; i++) {
//         if (!bdm_list[i]) {
//             continue;
//         }

//         free(bdm_list[i]->keys);
//         free(bdm_list[i]->key_lens);
//         free(bdm_list[i]);
//     }

//     free(bdm_list);

//     //Return the head of the list
//     return brm_head;
// }
