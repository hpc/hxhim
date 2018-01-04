/*
 * MDHIM TNG
 * 
 * External API and data structures
 */

#ifndef      __MDHIM_H
#define      __MDHIM_H

#include <stdint.h>
#include <pthread.h>
#include "data_store.h"
#include "range_server.h"
#include "messages.h"
#include "partitioner.h"
#include "mlog2.h"
#include "mlogfacs2.h"
#include "mdhim_options.h"
#include "indexes.h"
#include "mdhim_private.h"


#ifdef __cplusplus
extern "C"
{
#endif
#define MDHIM_SUCCESS 0
#define MDHIM_ERROR -1
#define MDHIM_DB_ERROR -2

#define SECONDARY_GLOBAL_INFO 1
#define SECONDARY_LOCAL_INFO 2

/**
 * Forward declare the private portions of the MDHIM data structure
 */
struct mdhim_private;

/* 
 * mdhim data 
 * Contains client communicator
 * Contains a list of range servers
 * Contains a pointer to mdhim_rs_t if rank is a range server
 */
typedef struct mdhim {
	// Opaque pointer to the private struct data
	struct mdhim_private *p;

    //Flag to indicate mdhimClose was called
    volatile int shutdown;
    //A pointer to the primary index
    struct index_t *primary_index;
    //A linked list of range servers
    struct index_t *indexes;
    // The hash to hold the indexes by name
    struct index_t *indexes_by_name;

    //Lock to allow concurrent readers and a single writer to the remote_indexes hash table
    pthread_rwlock_t *indexes_lock;

    //The range server structure which is used only if we are a range server
    mdhim_rs_t *mdhim_rs;
    //The mutex used if receiving from ourselves
    pthread_mutex_t *receive_msg_mutex;
    //The condition variable used if receiving from ourselves
    pthread_cond_t *receive_msg_ready_cv;
    /* The receive msg, which is sent to the client by the
       range server running in the same process */
    void *receive_msg;
    //Options for DB creation
    mdhim_options_t *db_opts;

} mdhim_t;

struct secondary_info {
	struct index_t *secondary_index;
	void **secondary_keys;
	int *secondary_key_lens;
	int num_keys;
	int info_type;
};

struct secondary_bulk_info {
	struct index_t *secondary_index;
	void ***secondary_keys;
	int **secondary_key_lens;
	int *num_keys;
	int info_type;
};

int mdhimInit(mdhim_t *mdh, mdhim_options_t *opts);
int mdhimClose(mdhim_t *md);
int mdhimCommit(mdhim_t *md, struct index_t *index);
int mdhimStatFlush(mdhim_t *md, struct index_t *index);
struct mdhim_brm_t *mdhimPut(mdhim_t *md,
			     void *key, int key_len,  
			     void *value, int value_len,  
			     struct secondary_info *secondary_global_info,
			     struct secondary_info *secondary_local_info);
struct mdhim_brm_t *mdhimPutSecondary(mdhim_t *md,
				      struct index_t *secondary_index,
				      /*Secondary key */
				      void *secondary_key, int secondary_key_len,  
				      /* Primary key */
				      void *primary_key, int primary_key_len);
struct mdhim_brm_t *mdhimBPut(mdhim_t *md,
			      void **primary_keys, int *primary_key_lens, 
			      void **primary_values, int *primary_value_lens, 
			      int num_records,
			      struct secondary_bulk_info *secondary_global_info,
			      struct secondary_bulk_info *secondary_local_info);
struct mdhim_bgetrm_t *mdhimGet(mdhim_t *md, struct index_t *index,
			       void *key, int key_len, 
			       int op);
struct mdhim_bgetrm_t *mdhimBGet(mdhim_t *md, struct index_t *index,
				 void **keys, int *key_lens, 
				 int num_records, int op);
struct mdhim_bgetrm_t *mdhimBGetOp(mdhim_t *md, struct index_t *index,
				   void *key, int key_len, 
				   int num_records, int op);
struct mdhim_brm_t *mdhimDelete(mdhim_t *md, struct index_t *index,
			       void *key, int key_len);
struct mdhim_brm_t *mdhimBDelete(mdhim_t *md, struct index_t *index,
				 void **keys, int *key_lens,
				 int num_keys);
void mdhim_release_recv_msg(void *msg);
struct secondary_info *mdhimCreateSecondaryInfo(struct index_t *secondary_index,
						void **secondary_keys, int *secondary_key_lens,
						int num_keys, int info_type);

void mdhimReleaseSecondaryInfo(struct secondary_info *si);
struct secondary_bulk_info *mdhimCreateSecondaryBulkInfo(struct index_t *secondary_index,
							 void ***secondary_keys, 
							 int **secondary_key_lens,
							 int *num_keys, int info_type);
void mdhimReleaseSecondaryBulkInfo(struct secondary_bulk_info *si);

#ifdef __cplusplus
}
#endif
#endif

