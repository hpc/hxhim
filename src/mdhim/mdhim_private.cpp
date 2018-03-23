#include <cstdlib>

#include "client.h"
#include "indexes.h"
#include "local_client.h"
#include "mdhim_private.h"
#include "partitioner.h"
#include "transport_mpi.hpp"
#include "transport_thallium.hpp"

int mdhim_private_init(mdhim_private* mdp, int dstype, int transporttype) {
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

    if (transporttype == MDHIM_TRANSPORT_MPI) {
        mdp->transport = new Transport(MPIInstance::instance().Rank());

        // create mapping between unique IDs and ranks
        for(int i = 0; i < MPIInstance::instance().Size(); i++) {
            mdp->transport->AddEndpoint(i, new MPIEndpoint(MPIInstance::instance().Comm(), i, mdp->shutdown));
        }

        mdp->listener_thread = MPIRangeServer::listener_thread;
        mdp->send_locally_or_remote = MPIRangeServer::send_locally_or_remote;

        rc = MDHIM_SUCCESS;
        goto err_out;
    }
    else if (transporttype == MDHIM_TRANSPORT_THALLIUM) {
        mdp->transport = new Transport(MPIInstance::instance().Rank());

        // give the thallium range server access to the mdhim data
        ThalliumRangeServer::init(mdp, "na+sm://127.0.0.1");

        // create mapping between unique IDs and thallium addresses
        // use MPI ranks as unique IDs
        for(int i = 0; i < MPIInstance::instance().Size(); i++) {
            mdp->transport->AddEndpoint(i, new ThalliumEndpoint("na+sm",
                                                                "na+sm://127.0.0.1:1234",
                                                                "na+sm://127.0.0.1:4321"));
        }

        rc = MDHIM_SUCCESS;
        goto err_out;
    }
    else {
        mlog(MDHIM_CLIENT_CRIT, "Invalid transport type specified");
        rc = MDHIM_ERROR;
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
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
			     "Error while determining range server in mdhimBPut",
			     md->p->transport->EndpointID());
			return NULL;
		}
	} else {
		//Get the range server this key will be sent to
		if ((rl = get_range_servers(md, lookup_index, key, key_len)) == NULL) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
			     "Error while determining range server in _put_record",
			     md->p->transport->EndpointID());
			return NULL;
		}
	}

    TransportRecvMessage *rm = nullptr;
	while (rl) {
        TransportPutMessage *pm = new TransportPutMessage();
		if (!pm) {
			mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
			     "Error while allocating memory in _put_record",
			     md->p->transport->EndpointID());
			return NULL;
		}

		//Initialize the put message
		pm->mtype = TransportMessageType::PUT;
		pm->key = key;
		pm->key_len = key_len;
		pm->value = value;
		pm->value_len = value_len;
        pm->src = md->p->transport->EndpointID();
		pm->dst = rl->ri->rank;
		pm->index = put_index->id;
		pm->index_type = put_index->type;

		//If I'm a range server and I'm the one this key goes to, send the message locally
		if (im_range_server(put_index) && md->p->transport->EndpointID() == pm->dst) {
			rm = local_client_put(md, pm);
		} else {
			//Send the message through the network as this message is for another rank
			rm = client_put(md, pm);
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
    if (!md || !index || !key || !key_len) {
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
        gm->src = md->p->transport->EndpointID();
        gm->dst = rl->ri->rank;
        gm->mtype = TransportMessageType::GET;
        gm->op = (op == TransportGetMessageOp::GET_PRIMARY_EQ)?TransportGetMessageOp::GET_EQ:op;
        gm->index = index->id;
        gm->index_type = index->type;

		//If I'm a range server and I'm the one this key goes to, send the message locally
		if (im_range_server(index) && md->p->transport->EndpointID() == gm->dst) {
			grm = local_client_get(md, gm);
		} else {
			//Send the message through the network as this message is for another rank
			grm = client_get(md, gm);
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
