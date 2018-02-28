#ifndef MDHIM_PRIVATE_H
#define MDHIM_PRIVATE_H

#include "mdhim.h"
#include "range_server.h"

/**
 * Struct that contains the private details about MDHim's implementation
 */
typedef struct mdhim_private mdhim_private_t;

#ifdef __cplusplus
extern "C"
{
#endif
typedef struct mdhim {
	//This communicator will include every process in the application, but is separate from main the app
    //It is used for sending and receiving to and from the range servers
	MPI_Comm mdhim_comm;
	pthread_mutex_t mdhim_comm_lock;

	//This communicator will include every process in the application, but is separate from the app
    //It is used for barriers for clients
	MPI_Comm mdhim_client_comm;

	//The rank in the mdhim_comm
	int mdhim_rank;
	//The size of mdhim_comm
	int mdhim_comm_size;
	//Flag to indicate mdhimClose was called
	volatile int shutdown;
	//A pointer to the primary index
	struct index_t *primary_index;
	//A linked list of range servers
	struct index_t *indexes;
	// The hash to hold the indexes by name
	struct index_t *indexes_by_name;

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

	/* // Opaque pointer to the private portions of this struct */
	/* struct mdhim_private *p; */
} mdhim_t;

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

struct mdhim_rm_t *_put_record(struct mdhim *md, struct index_t *index,
			       void *key, int key_len,
			       void *value, int value_len);
struct mdhim_brm_t *_create_brm(struct mdhim_rm_t *rm);
void _concat_brm(struct mdhim_brm_t *head, struct mdhim_brm_t *addition);
struct mdhim_brm_t *_bput_records(struct mdhim *md, struct index_t *index,
				  void **keys, int *key_lens,
				  void **values, int *value_lens, int num_records);
struct mdhim_bgetrm_t *_bget_records(struct mdhim *md, struct index_t *index,
				     void **keys, int *key_lens,
				     int num_keys, int num_records, int op);
struct mdhim_brm_t *_bdel_records(struct mdhim *md, struct index_t *index,
				  void **keys, int *key_lens,
				  int num_records);

#endif
