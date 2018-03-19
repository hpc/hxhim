#ifndef MDHIM_PRIVATE_H
#define MDHIM_PRIVATE_H

#include <functional>

#include "mdhim.h"
#include "range_server.h"
#include "transport.hpp"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Struct that contains the private details about MDHim's implementation
 */
typedef struct mdhim_private {
	//This communicator will include every process in the application, but is separate from main the app
    //It is used for sending and receiving to and from the range servers
    Transport *transport;
	MPI_Comm mdhim_comm;
	pthread_mutex_t mdhim_comm_lock;

    // function called in the listener thread to receive data
    TransportWorkReceiver work_receiver;

    // function called by the worker thread after processing the received data
    TransportResponseSender response_sender;

	//This communicator will include every process in the application, but is separate from the app
    //It is used for barriers for clients
	MPI_Comm mdhim_client_comm;

	/* //The rank in the mdhim_comm */
	/* int mdhim_rank; */
	//The size of mdhim_comm
	int mdhim_comm_size;
	//Flag to indicate mdhimClose was called
	volatile int shutdown;
	//A pointer to the primary index
	index_t *primary_index;
	//A linked list of range servers
	index_t *indexes;
	// The hash to hold the indexes by name
	index_t *indexes_by_name;

	//Lock to allow concurrent readers and a single writer to the remote_indexes hash table
	pthread_rwlock_t indexes_lock;

	//The range server structure which is used only if we are a range server
	mdhim_rs_t *mdhim_rs;
	//The mutex used if receiving from ourselves
	pthread_mutex_t receive_msg_mutex;
	//The condition variable used if receiving from ourselves
	pthread_cond_t receive_msg_ready_cv;
	/* The receive msg, which is sent to the client by the
	   range server running in the same process */
	void *receive_msg;
    //Options for DB creation
	mdhim_options_t *db_opts;
} mdhim_private_t;
#ifdef __cplusplus
}
#endif

/**
 *
 * @param mdp An allocated MDHim private data structure
 * @param dstype The data store type to instantiate
 * @param commtype The communication type to instantiate
 * @return 0 on success, non-zero on failre
 */
int mdhim_private_init(struct mdhim_private* mdp, int dstype, int commtype);

TransportRecvMessage *_put_record(mdhim_t *md, index_t *index,
                                   void *key, int key_len,
                                   void *value, int value_len);
TransportBRecvMessage *_create_brm(TransportRecvMessage *rm);
void _concat_brm(TransportBRecvMessage *head, TransportBRecvMessage *addition);
TransportBRecvMessage *_bput_records(mdhim_t *md, index_t *index,
                                     void **keys, int *key_lens,
                                     void **values, int *value_lens,
                                     int num_records);
TransportBGetRecvMessage *_bget_records(mdhim_t *md, index_t *index,
                                        void **keys, int *key_lens,
                                        int num_keys, int num_records,
                                        TransportGetMessageOp op);
// mdhim_brm_t *_bdel_records(mdhim_t *md, index_t *index,
//                            void **keys, int *key_lens,
//                            int num_records);

#endif
