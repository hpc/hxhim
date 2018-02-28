#ifndef      __RANGESRV_H
#define      __RANGESRV_H

#include <pthread.h>
#include <mpi.h>

#include "indexes.h"
#include "mdhim.h"
#include "messages.h"
#include "data_store.h"

typedef struct work_item work_item_t;

struct work_item {
	work_item_t *next;
	work_item_t *prev;
	void *message;
	int source;
};

typedef struct work_queue_t {
	work_item_t *head;
	work_item_t *tail;
} work_queue_t;

/* Outstanding requests (i.e., MPI_Req) that need to be freed later */
typedef struct out_req out_req;
struct out_req {
	out_req *next;
	out_req *prev;
	void *req;
	MPI_Request *message;
};

/* Range server specific data */
typedef struct mdhim_rs_t {
	work_queue_t *work_queue;
	pthread_mutex_t *work_queue_mutex;
	pthread_cond_t *work_ready_cv;
	pthread_t listener;
	pthread_t **workers;
	struct index *indexes; /* A linked list of remote indexes that is served
				  (partially for fully) by this range server */
	//Records seconds spent on putting records
	long double put_time;
	//Records seconds spend on getting records
	long double get_time;
	long num_put;
	long num_get;
	out_req *out_req_list;
	pthread_mutex_t *out_req_mutex;
} mdhim_rs_t;

int range_server_add_work(struct mdhim *md, work_item_t *item);
int range_server_init(struct mdhim *md);
int range_server_init_comm(struct mdhim *md);
int range_server_stop(struct mdhim *md);
int range_server_add_oreq(struct mdhim *md, MPI_Request *req, void *msg); //Add an outstanding request
int range_server_clean_oreqs(struct mdhim *md); //Clean outstanding reqs

#endif
