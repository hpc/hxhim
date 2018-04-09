#include <cstdlib>
#include <map>
#include <memory>
#include <unistd.h>
#include <sys/types.h>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "indexes.h"
#include "local_client.h"
#include "mdhim_options_private.h"
#include "mdhim_private.h"
#include "partitioner.h"
#include "transport_mpi.hpp"
#include "transport_thallium.hpp"

// get all thallium lookup addresses
static int get_addrs(thallium::engine *engine, const MPI_Comm comm, std::map<int, std::string> &addrs) {
    if (!engine) {
        return MDHIM_ERROR;
    }

    int rank;
    if (MPI_Comm_rank(comm, &rank) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }

    int size;
    if (MPI_Comm_size(comm, &size) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }

    // get local engine's address
    const std::string self = (std::string) engine->self();

    // get maximum size of all addresses
    const int self_len = self.size();
    int max_len = 0;
    if (MPI_Allreduce(&self_len, &max_len, 1, MPI_INT, MPI_MAX, comm) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }
    max_len++; // nullptr terminate

    // get addresses
    char *buf = new char[max_len * size]();
    if (MPI_Allgather(self.c_str(), self.size(), MPI_CHAR, buf, max_len, MPI_CHAR, MPI_COMM_WORLD) != MPI_SUCCESS) {
        delete [] buf;
        return MDHIM_ERROR;
    }

    // copy the addresses into strings
    // and map the strings to unique IDs
    for(int i = 0; i < size; i++) {
        const char *remote = &buf[max_len * i];
        addrs[i].assign(remote, strlen(remote));
    }

    delete [] buf;

    return MDHIM_SUCCESS;
}

int mdhim_private_init(mdhim_private_t* mdp, int dbtype, int transporttype) {
    int rc = MDHIM_ERROR;
    if (!mdp) {
        // goto err_out;
        return MDHIM_ERROR;
    }

    if (dbtype == LEVELDB) {
        rc = MDHIM_SUCCESS;
    }
    else {
        mlog(MDHIM_CLIENT_CRIT, "Invalid data store type specified");
        rc = MDHIM_DB_ERROR;
        goto err_out;
    }

    mdp->transport = new Transport();

    if (transporttype == MDHIM_TRANSPORT_MPI) {
        TransportEndpointGroup *eg = new MPIEndpointGroup(mdp, mdp->shutdown);

        // create mapping between unique IDs and ranks
        for(int i = 0; i < mdp->mdhim_comm_size; i++) {
            mdp->transport->AddEndpoint(i, new MPIEndpoint(mdp->mdhim_comm, i, mdp->shutdown));
            eg->AddEndpoint(i, new MPIEndpoint(mdp->mdhim_comm, i, mdp->shutdown));
        }

        // remove loopback endpoint
        mdp->transport->RemoveEndpoint(mdp->mdhim_rank);

        mdp->transport->SetEndpointGroup(eg);
        mdp->listener_thread = MPIRangeServer::listener_thread;
        mdp->send_client_response = MPIRangeServer::send_client_response;

        rc = MDHIM_SUCCESS;
    }
    else if (transporttype == MDHIM_TRANSPORT_THALLIUM) {
        // create the engine (only 1 instance per process)
        thallium::engine *engine = new thallium::engine("na+sm", THALLIUM_SERVER_MODE, true, -1);

        // create client to range server RPC
        thallium::remote_procedure *rpc = new thallium::remote_procedure(engine->define(ThalliumRangeServer::CLIENT_TO_RANGE_SERVER_NAME,
                                                                                        ThalliumRangeServer::receive_rangesrv_work));

        // give the range server access to the mdhim_t data
        ThalliumRangeServer::init(mdp);

        // wait for every engine to start up
        MPI_Barrier(mdp->mdhim_comm);

        // get a mapping of unique IDs to thallium addresses
        std::map<int, std::string> addrs;
        if (get_addrs(engine, mdp->mdhim_comm, addrs) != MDHIM_SUCCESS) {
            rc = MDHIM_ERROR;
            goto err_out;
        }

        // do not remove loopback endpoint - need to keep a reference to engine somewhere

        // add the remote thallium endpoints to the tranport
        for(std::pair<const int, std::string> const &addr : addrs) {
            thallium::endpoint *server = new thallium::endpoint(engine->lookup(addr.second));
            ThalliumEndpoint* ep = new ThalliumEndpoint(engine, rpc, server);
            mdp->transport->AddEndpoint(addr.first, ep);
        }

        mdp->listener_thread = nullptr;
        mdp->send_client_response = ThalliumRangeServer::send_client_response;

        rc = MDHIM_SUCCESS;
    }
    else {
        mlog(MDHIM_CLIENT_CRIT, "Invalid transport type specified");
        rc = MDHIM_ERROR;
    }

err_out:
    return rc;
}

TransportRecvMessage *_put_record(mdhim_t *md, index_t *index,
                                  void *key, int key_len,
                                  void *value, int value_len) {
    if (!md || !md->p ||
        !index ||
        !key || !key_len ||
        !value || !value_len) {
        return nullptr;
    }

    rangesrv_list *rl;
    index_t *lookup_index, *put_index;

    put_index = index;
    if (index->type == LOCAL_INDEX) {
        lookup_index = get_index(md, index->primary_id);
        if (!lookup_index) {
            return nullptr;
        }
    } else {
        lookup_index = index;
    }

    //Get the range server this key will be sent to
    if (put_index->type == LOCAL_INDEX) {
        if ((rl = get_range_servers(md, lookup_index, value, value_len)) ==
            nullptr) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in mdhimBPut",
                  md->p->mdhim_rank);
            return nullptr;
        }
    } else {
        //Get the range server this key will be sent to
        if ((rl = get_range_servers(md, lookup_index, key, key_len)) == nullptr) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in _put_record",
                  md->p->mdhim_rank);
            return nullptr;
        }
    }

    TransportRecvMessage *rm = nullptr;
    while (rl) {
        TransportPutMessage *pm = new TransportPutMessage();
        if (!pm) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while allocating memory in _put_record",
                  md->p->mdhim_rank);
            return nullptr;
        }

        //Initialize the put message
        pm->mtype = TransportMessageType::PUT;
        pm->key = key;
        pm->key_len = key_len;
        pm->value = value;
        pm->value_len = value_len;
        pm->src =  md->p->mdhim_rank;
        pm->dst = rl->ri->rank;
        pm->index = put_index->id;
        pm->index_type = put_index->type;

        //If I'm a range server and I'm the one this key goes to, send the message locally
        if (im_range_server(put_index) &&  md->p->mdhim_rank == pm->dst) {
            rm = local_client_put(md, pm);
        } else {
            //Send the message through the network as this message is for another rank
            rm = md->p->transport->Put(pm);
            delete pm;
        }

        rangesrv_list *rlp = rl;
        rl = rl->next;
        free(rlp);
    }

    return rm;
}

TransportGetRecvMessage *_get_record(mdhim_t *md, index_t *index,
                                     void *key, int key_len,
                                     enum TransportGetMessageOp op) {
    if (!md || !md->p ||
        !index ||
        !key || !key_len) {
        return nullptr;
    }

    //Create an array of bulk get messages that holds one bulk message per range server
    rangesrv_list *rl = nullptr;
    //Get the range server this key will be sent to
    if ((op == TransportGetMessageOp::GET_EQ || op == TransportGetMessageOp::GET_PRIMARY_EQ) &&
        index->type != LOCAL_INDEX &&
        (rl = get_range_servers(md, index, key, key_len)) ==
        nullptr) {
        return nullptr;
    } else if ((index->type == LOCAL_INDEX ||
                (op != TransportGetMessageOp::GET_EQ && op != TransportGetMessageOp::GET_PRIMARY_EQ)) &&
               (rl = get_range_servers_from_stats(md, index, key, key_len, op)) ==
               nullptr) {
        return nullptr;
    }

    TransportGetRecvMessage *grm = nullptr;
    while (rl) {
        TransportGetMessage *gm = new TransportGetMessage();
        gm->key = key;
        gm->key_len = key_len;
        gm->num_keys = 1;
        gm->src = md->p->mdhim_rank;
        gm->dst = rl->ri->rank;
        gm->mtype = TransportMessageType::GET;
        gm->op = (op == TransportGetMessageOp::GET_PRIMARY_EQ)?TransportGetMessageOp::GET_EQ:op;
        gm->index = index->id;
        gm->index_type = index->type;

        //If I'm a range server and I'm the one this key goes to, send the message locally
        if (im_range_server(index) &&  md->p->mdhim_rank == gm->dst) {
            grm = local_client_get(md, gm);
        } else {
            //Send the message through the network as this message is for another rank
            grm = md->p->transport->Get(gm);
            delete gm;
        }

        rangesrv_list *rlp = rl;
        rl = rl->next;
        free(rlp);
    }

    return grm;
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
    brm->dst = rm->dst;

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
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "To many bulk operations requested in mdhimBGetOp",
              md->p->mdhim_rank);
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
                mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                     "Error while determining range server in mdhimBPut",
                      md->p->mdhim_rank);
                continue;
            }
        } else {
            if ((rl = get_range_servers(md, lookup_index, keys[i], key_lens[i])) ==
                nullptr) {
                mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                     "Error while determining range server in mdhimBPut",
                      md->p->mdhim_rank);
                continue;
            }
        }

        //There could be more than one range server returned in the case of the local index
        while (rl) {
            TransportBPutMessage *bpm = nullptr;
            if (rl->ri->rank != md->p->mdhim_rank) {
                //Set the message in the list for this range server
                bpm = bpm_list[rl->ri->rangesrv_num - 1];
            } else {
                //Set the local message
                bpm = lbpm;
            }

            //If the message doesn't exist, create one
            if (!bpm) {
                bpm = new TransportBPutMessage();
                bpm->keys = new void *[MAX_BULK_OPS]();
                bpm->key_lens = new int[MAX_BULK_OPS]();
                bpm->values = new void *[MAX_BULK_OPS]();
                bpm->value_lens = new int[MAX_BULK_OPS]();
                bpm->num_keys = 0;
                bpm->src =  md->p->mdhim_rank;
                bpm->dst = rl->ri->rank;
                bpm->mtype = TransportMessageType::BPUT;
                bpm->index = put_index->id;
                bpm->index_type = put_index->type;
                if (rl->ri->rank !=  md->p->mdhim_rank) {
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
    TransportBRecvMessage *brm_head = md->p->transport->BPut(put_index->num_rangesrvs, bpm_list);
    if (lbpm) {
        TransportRecvMessage *rm = local_client_bput(md, lbpm);
        if (rm) {
            TransportBRecvMessage *brm = _create_brm(rm);
            brm->next = brm_head;
            brm_head = brm;
            delete rm;
        }
    }

    //Free up messages sent
    for (int i = 0; i < lookup_index->num_rangesrvs; i++) {
        delete bpm_list[i];
    }

    // free(bpm_list);
    delete [] bpm_list;

    //Return the head of the list
    return brm_head;
}

TransportBGetRecvMessage *_bget_records(mdhim_t *md, index_t *index,
                                        void **keys, int *key_lens,
                                        int num_keys, int num_records,
                                        enum TransportGetMessageOp op) {
    if (!md || !md->p ||
        !index ||
        !keys || !key_lens) {
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
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                 "Error while determining range server in mdhimBget",
                 md->p->mdhim_rank);
            delete [] bgm_list;
            return nullptr;
        } else if ((index->type == LOCAL_INDEX ||
                    (op != TransportGetMessageOp::GET_EQ && op != TransportGetMessageOp::GET_PRIMARY_EQ)) &&
                   (rl = get_range_servers_from_stats(md, index, keys[i], key_lens[i], op)) ==
                   nullptr) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                 "Error while determining range server in mdhimBget",
                 md->p->mdhim_rank);
            delete [] bgm_list;
            return nullptr;
        }

        while (rl) {
            TransportBGetMessage *bgm = nullptr;
            if (rl->ri->rank != md->p->mdhim_rank) {
                //Set the message in the list for this range server
                bgm = bgm_list[rl->ri->rangesrv_num - 1];
            } else {
                bgm = lbgm;
            }

            //If the message doesn't exist, create one
            if (!bgm) {
                bgm = new TransportBGetMessage();
                bgm->keys = new void *[num_keys]();
                bgm->key_lens = new int[num_keys]();
                bgm->num_keys = 0;
                bgm->num_recs = num_records;
                bgm->src = md->p->mdhim_rank;
                bgm->dst = rl->ri->rank;
                bgm->mtype = TransportMessageType::BGET;
                bgm->op = (op == TransportGetMessageOp::GET_PRIMARY_EQ)?TransportGetMessageOp::GET_EQ:op;
                bgm->index = index->id;
                bgm->index_type = index->type;
                if (rl->ri->rank != md->p->mdhim_rank) {
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
    TransportBGetRecvMessage *bgrm_head = md->p->transport->BGet(index->num_rangesrvs, bgm_list);
    if (lbgm) {
        TransportBGetRecvMessage *lbgrm = local_client_bget(md, lbgm);
        lbgrm->next = bgrm_head;
        bgrm_head = lbgrm;
    }

    delete [] bgm_list;

    return bgrm_head;
}

/**
 * Deletes multiple records from MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to delete
 * @param key_lens     array with lengths of each key in keys
 * @param num_keys  the number of keys to delete (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or nullptr on error
 */
TransportBRecvMessage *_bdel_records(mdhim_t *md, index_t *index,
                                     void **keys, int *key_lens,
                                     int num_keys) {
    if (!md || !md->p ||
        !index ||
        !keys || !key_lens) {
        return nullptr;
    }

	//The message to be sent to ourselves if necessary
    TransportBDeleteMessage *lbdm = nullptr;

	//Create an array of bulk del messages that holds one bulk message per range server
    TransportBDeleteMessage **bdm_list = new TransportBDeleteMessage *[index->num_rangesrvs]();

	rangesrv_list *rl = nullptr;

	/* Go through each of the records to find the range server the record belongs to.
	   If there is not a bulk message in the array for the range server the key belongs to,
	   then it is created.  Otherwise, the data is added to the existing message in the array.*/
	for (int i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
		//Get the range server this key will be sent to
		if (index->type != LOCAL_INDEX &&
		    !(rl = get_range_servers(md, index, keys[i], key_lens[i]))) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
			     "Error while determining range server in mdhimBdel",
			     md->p->mdhim_rank);
			continue;
		} else if (index->type == LOCAL_INDEX &&
                   !(rl = get_range_servers_from_stats(md, index, keys[i], key_lens[i], TransportGetMessageOp::GET_EQ))) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
			     "Error while determining range server in mdhimBdel",
			     md->p->mdhim_rank);
			continue;
		}

        TransportBDeleteMessage *bdm = nullptr;
		if (rl->ri->rank != md->p->mdhim_rank) {
			//Set the message in the list for this range server
			bdm = bdm_list[rl->ri->rangesrv_num - 1];
		} else {
			//Set the local message
			bdm = lbdm;
		}

		//If the message doesn't exist, create one
		if (!bdm) {
            bdm = new TransportBDeleteMessage();
            bdm->keys = new void *[MAX_BULK_OPS]();
            bdm->key_lens = new int[MAX_BULK_OPS]();
			bdm->num_keys = 0;
            bdm->src = md->p->mdhim_rank;
			bdm->dst = rl->ri->rank;
			bdm->mtype = TransportMessageType::BDELETE;
			bdm->index = index->id;
			bdm->index_type = index->type;
			if (rl->ri->rank != md->p->mdhim_rank) {
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
	TransportBRecvMessage *brm_head = md->p->transport->BDelete(index->num_rangesrvs, bdm_list);
	if (lbdm) {
		TransportRecvMessage *rm = local_client_bdelete(md, lbdm);
		TransportBRecvMessage *brm = new TransportBRecvMessage();
		brm->error = rm->error;
		brm->mtype = rm->mtype;
		brm->index = rm->index;
		brm->index_type = rm->index_type;
		brm->src = rm->src;
		brm->dst = rm->dst;
		brm->next = brm_head;
		brm_head = brm;
        delete rm;
	}

	for (int i = 0; i < index->num_rangesrvs; i++) {
        delete bdm_list[i];
	}

    delete [] bdm_list;

	//Return the head of the list
	return brm_head;
}

int _which_server(struct mdhim *md, void *key, int key_len)
{
    rangesrv_list *rl = get_range_servers(md, md->p->primary_index, key, key_len);
    int server = rl?rl->ri->rank:MDHIM_ERROR;
    free(rl);
    /* what is the difference between 'rank' and 'rangeserv_num' ? */
    return server;
}
