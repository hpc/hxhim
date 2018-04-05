/*
 * MDHIM TNG
 *
 * Client code for sending to and receiving from yourself
 */

#include <cstdlib>

#include "mdhim_private.h"
#include "local_client.h"
#include "transport_private.hpp"
#include "MemoryManagers.hpp"

/**
 * get_msg_self
 * Gets a message from the range server if we are waiting to hear back from ourselves
 * This means that the range server is running in the same process as the caller,
 * but on a different thread
 *
 * @param md the main mdhim struct
 * @return a pointer to the message received or NULL
 */
static void *get_msg_self(struct mdhim *md) {
	//Lock the receive msg mutex
	pthread_mutex_lock(&md->p->receive_msg_mutex);
	//Wait until there is a message to receive
	if (!md->p->receive_msg) {
		pthread_cond_wait(&md->p->receive_msg_ready_cv, &md->p->receive_msg_mutex);
	}

	//Get the message
	void *msg = md->p->receive_msg;
	//Set the message queue to null
	md->p->receive_msg = NULL;
	//unlock the mutex
	pthread_mutex_unlock(&md->p->receive_msg_mutex);

	return msg;
}

/**
 * Send put to range server
 *
 * @param md main MDHIM struct
 * @param pm pointer to put message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
TransportRecvMessage *local_client_put(mdhim_t *md, TransportPutMessage *pm) {
	work_item_t *item = Memory::FBP_MEDIUM::Instance().acquire<work_item_t>();

	if (!item) {
		return nullptr;
	}

    // This needs the double static_cast in order for it to work properly
    // This is probably a clang++ 3.9.1 bug
    item->message = static_cast<TransportMessage *>(pm);
    item->address = pm->dst;

	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
		return nullptr;
	}

    return dynamic_cast<TransportRecvMessage *>(static_cast<TransportMessage *>(get_msg_self(md)));
}

TransportGetRecvMessage *local_client_get(mdhim_t *md, TransportGetMessage *gm) {
	work_item_t *item = Memory::FBP_MEDIUM::Instance().acquire<work_item_t>();

	if (!item) {
		return nullptr;
	}

    // This needs the double static_cast in order for it to work properly
    // This is probably a clang++ 3.9.1 bug
    item->message = static_cast<TransportMessage *>(gm);
    item->address = gm->dst;

	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
		return nullptr;
	}

    return dynamic_cast<TransportGetRecvMessage *>(static_cast<TransportMessage *>(get_msg_self(md)));
}

// /**
//  * Send bulk put to range server
//  *
//  * @param md main MDHIM struct
//  * @param bpm pointer to bulk put message to be sent or inserted into the range server's work queue
//  * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
// */
// TransportRecvMessage *local_client_bput(struct mdhim *md, TransportBPutMessage *bpm) {
// 	work_item_t *item = Memory::FBP_MEDIUM::Instance().acquire<work_item_t>();

// 	if (!item) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
// 		return NULL;
// 	}

//     item->message = static_cast<TransportMessage *>(bpm);
//     item->address = md->p->transport->Endpoint()->Address();

// 	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_put");
// 		return NULL;
// 	}

//     return dynamic_cast<TransportRecvMessage *>(static_cast<TransportMessage *>(get_msg_self(md)));
// }

// /**
//  * Send bulk get to range server
//  *
//  * @param md main MDHIM struct
//  * @param bgm pointer to get message to be sent or inserted into the range server's work queue
//  * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
//  */
// TransportBGetRecvMessage *local_client_bget(struct mdhim *md, TransportBGetMessage *bgm) {
// 	work_item_t *item = Memory::FBP_MEDIUM::Instance().acquire<work_item_t>();

// 	if (!item) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
// 		return NULL;
// 	}

// 	item->message = static_cast<TransportMessage *>(bgm);
//     item->address = md->p->transport->Endpoint()->Address();

// 	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_put");
// 		return NULL;
// 	}

//     return static_cast<TransportBGetRecvMessage *>(get_msg_self(md));
// }

// /**
//  * Send get with an op and number of records greater than 1 to range server
//  *
//  * @param md main MDHIM struct
//  * @param gm pointer to get message to be inserted into the range server's work queue
//  * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
//  */
// TransportBGetRecvMessage *local_client_bget_op(struct mdhim *md, TransportGetMessage *gm) {
// 	work_item_t *item = Memory::FBP_MEDIUM::Instance().acquire<work_item_t>();

// 	if (!item) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
// 		return NULL;
// 	}

// 	item->message = static_cast<TransportMessage *>(gm);
//  item->address = md->p->transport->Endpoint()->Address();
// 	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_bput");
// 		return NULL;
// 	}

//     return static_cast<TransportBGetRecvMessage *>(get_msg_self(md));
// }

/**
 * Send commit to range server
 *
 * @param md main MDHIM struct
 * @param cm pointer to put message to be inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
TransportRecvMessage *local_client_commit(mdhim_t *md, TransportMessage *cm) {
 	work_item_t *item = Memory::FBP_MEDIUM::Instance().acquire<work_item_t>();

	if (!item) {
		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
		return nullptr;
	}

	item->message = cm;
    item->address = cm->dst;

	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_put");
		return nullptr;
	}

    return dynamic_cast<TransportRecvMessage *>(static_cast<TransportMessage *>(get_msg_self(md)));
}

// /**
//  * Send delete to range server
//  *
//  * @param md main MDHIM struct
//  * @param dm pointer to delete message to be inserted into the range server's work queue
//  * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
//  */
// struct mdhim_rm_t *local_client_delete(struct mdhim *md, struct mdhim_delm_t *dm) {
// 	int ret;
// 	struct mdhim_rm_t *rm;
// 	work_item_t *item = Memory::FBP_MEDIUM::Instance().acquire<work_item_t>();

// 	if (!item) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
// 		return NULL;
// 	}

// 	item->message = (void *)dm;
//  item->address = md->p->transport->Endpoint()->Address();

// 	if ((ret = range_server_add_work(md, item)) != MDHIM_SUCCESS) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_put");
// 		return NULL;
// 	}

// 	rm = (struct mdhim_rm_t *) get_msg_self(md);

// 	// Return response
// 	return rm;

// }

// /**
//  * Send bulk delete to MDHIM
//  *
//  * @param md main MDHIM struct
//  * @param bdm pointer to bulk delete message to be inserted into the range server's work queue
//  * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
//  */
// struct mdhim_rm_t *local_client_bdelete(struct mdhim *md, struct mdhim_bdelm_t *bdm) {
// 	int ret;
// 	struct mdhim_rm_t *brm;
// 	work_item_t *item = Memory::FBP_MEDIUM::Instance().acquire<work_item_t>();

// 	if (!item) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
// 		return NULL;
// 	}

// 	item->message = (void *)bdm;
//  item->address = md->p->transport->Endpoint()->Address();

// 	if ((ret = range_server_add_work(md, item)) != MDHIM_SUCCESS) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_put");
// 		return NULL;
// 	}

// 	brm = (struct mdhim_rm_t *) get_msg_self(md);

// 	// Return response
// 	return brm;
// }

// /**
//  * Send close to range server
//  *
//  * @param md main MDHIM struct
//  * @param cm pointer to close message to be inserted into the range server's work queue
//  */
// void local_client_close(struct mdhim *md, struct mdhim_basem_t *cm) {
// 	int ret;
// 	work_item_t *item = Memory::FBP_MEDIUM::Instance().acquire<work_item_t>();

// 	if (!item) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
// 		return;
// 	}

// 	item->message = (void *)cm;
//  item->address = md->p->transport->Endpoint()->Address();

// 	if ((ret = range_server_add_work(md, item)) != MDHIM_SUCCESS) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_put");
// 		return;
// 	}

// 	return;
// }
