/*
 * MDHIM TNG
 *
 * Client code for sending to and receiving from yourself
 */

#include <cstdlib>

#include "mdhim_private.h"
#include "local_client.h"
#include "transport_private.hpp"

/**
 * get_msg_self
 * Gets a message from the range server if we are waiting to hear back from ourselves
 * This means that the range server is running in the same process as the caller,
 * but on a different thread
 *
 * @param md the main mdhim struct
 * @return a pointer to the message received or NULL
 */
static TransportMessage *get_msg_self(mdhim_t *md) {
	//Lock the receive msg mutex
	pthread_mutex_lock(&md->p->receive_msg_mutex);

	//Wait until there is a message to receive
	if (!md->p->receive_msg) {
		pthread_cond_wait(&md->p->receive_msg_ready_cv, &md->p->receive_msg_mutex);
	}

	//Get the message
	TransportMessage *msg = md->p->receive_msg;

	//Set the message queue to null
	md->p->receive_msg = nullptr;

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
	work_item_t *item = new work_item_t();

	if (!item) {
		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
		return nullptr;
	}

    item->message = pm;

	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_put");
		return nullptr;
	}

    return dynamic_cast<TransportRecvMessage *>(static_cast<TransportMessage *>(get_msg_self(md)));
}

TransportGetRecvMessage *local_client_get(mdhim_t *md, TransportGetMessage *gm) {
	work_item_t *item = new work_item_t();

	if (!item) {
		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
		return nullptr;
	}

    item->message = gm;

	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_get");
		return nullptr;
	}

    return dynamic_cast<TransportGetRecvMessage *>(static_cast<TransportMessage *>(get_msg_self(md)));
}

/**
 * Send bulk put to range server
 *
 * @param md main MDHIM struct
 * @param bpm pointer to bulk put message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
*/
TransportRecvMessage *local_client_bput(mdhim_t *md, TransportBPutMessage *bpm) {
	work_item_t *item = new work_item_t();

	if (!item) {
		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
		return NULL;
	}

    item->message = bpm;

	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_bput");
		return NULL;
	}

    return dynamic_cast<TransportRecvMessage *>(static_cast<TransportMessage *>(get_msg_self(md)));
}

/**
 * Send bulk get to range server
 *
 * @param md main MDHIM struct
 * @param bgm pointer to get message to be sent or inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
TransportBGetRecvMessage *local_client_bget(mdhim_t *md, TransportBGetMessage *bgm) {
	work_item_t *item = new work_item_t();

	if (!item) {
		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
		return NULL;
	}

	item->message = bgm;

	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_bget");
		return NULL;
	}

    return dynamic_cast<TransportBGetRecvMessage *>(static_cast<TransportMessage *>(get_msg_self(md)));
}

/**
 * Send get with an op and number of records greater than 1 to range server
 *
 * @param md main MDHIM struct
 * @param gm pointer to get message to be inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
TransportBGetRecvMessage *local_client_bget_op(mdhim_t *md, TransportGetMessage *gm) {
	work_item_t *item = new work_item_t();

	if (!item) {
		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
		return NULL;
	}

	item->message = gm;

	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_bput");
		return NULL;
	}

    return dynamic_cast<TransportBGetRecvMessage *>(get_msg_self(md));
}

/**
 * Send commit to range server
 *
 * @param md main MDHIM struct
 * @param cm pointer to put message to be inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
TransportRecvMessage *local_client_commit(mdhim_t *md, TransportMessage *cm) {
 	work_item_t *item = new work_item_t();

	if (!item) {
		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
		return nullptr;
	}

	item->message = cm;

	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_commit");
		return nullptr;
	}

    return dynamic_cast<TransportRecvMessage *>(static_cast<TransportMessage *>(get_msg_self(md)));
}

/**
 * Send delete to range server
 *
 * @param md main MDHIM struct
 * @param dm pointer to delete message to be inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
TransportRecvMessage *local_client_delete(mdhim_t *md, TransportBDeleteMessage *dm) {
	work_item_t *item = new work_item_t();

	if (!item) {
		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
		return NULL;
	}

	item->message = dm;

	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_delete");
		return NULL;
	}

    return dynamic_cast<TransportRecvMessage *>(get_msg_self(md));
}

/**
 * Send bulk delete to MDHIM
 *
 * @param md main MDHIM struct
 * @param bdm pointer to bulk delete message to be inserted into the range server's work queue
 * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
 */
TransportRecvMessage *local_client_bdelete(mdhim_t *md, TransportBDeleteMessage *bdm) {
	work_item_t *item = new work_item_t();

	if (!item) {
		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
		return NULL;
	}

	item->message = bdm;

	if (range_server_add_work(md, item) != MDHIM_SUCCESS) {
		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_bdelete");
		return NULL;
	}

    return dynamic_cast<TransportRecvMessage *>(get_msg_self(md));
}

// /**
//  * Send close to range server
//  *
//  * @param md main MDHIM struct
//  * @param cm pointer to close message to be inserted into the range server's work queue
//  */
// void local_client_close(mdhim_t *md, TransportMessage *cm) {
// 	int ret;
// 	work_item_t *item = new work_item_t();

// 	if (!item) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error while allocating memory for client");
// 		return;
// 	}

// 	item->message = cm;

// 	if ((ret = range_server_add_work(md, item)) != MDHIM_SUCCESS) {
// 		mlog(MDHIM_CLIENT_CRIT, "Error adding work to range server in local_client_put");
// 		return;
// 	}

// 	return;
// }
