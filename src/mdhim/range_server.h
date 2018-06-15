#ifndef      __RANGESRV_H
#define      __RANGESRV_H

#include <pthread.h>

#include <mpi.h>

#include "transport.h"
#include "indexes.h"
#include "mdhim.h"
#include "data_store.h"
#include "work_item.h"

typedef struct work_queue {
    work_item_t *head;
    work_item_t *tail;
} work_queue_t;

/* Outstanding requests (i.e., MPI_Req) that need to be freed later */
typedef struct out_req {
    out_req *next;
    out_req *prev;
    MPI_Request *req;
    void *message;
} out_req_t;

/* Range server specific data */
typedef struct mdhim_rs {
    work_queue_t *work_queue;
    pthread_mutex_t work_queue_mutex;
    pthread_cond_t work_ready_cv;
    pthread_t *listener;
    pthread_t *workers;
    index_t *indexes; /* A linked list of remote indexes that is served
                         (partially for fully) by this range server */

    // Statistics
    pthread_mutex_t stat_mutex;
    long double put_time; // seconds
    long double get_time; // seconds
    std::size_t num_puts;
    std::size_t num_gets;

    out_req_t *out_req_list;
    pthread_mutex_t out_req_mutex;
} mdhim_rs_t;

int range_server_add_work(mdhim_t *md, work_item_t *item);
int range_server_init(mdhim_t *md);
int range_server_init_comm(mdhim_t *md);
int range_server_stop(mdhim_t *md);
int range_server_add_oreq(mdhim_t *md, MPI_Request *req, void *msg); //Add an outstanding request
int range_server_clean_oreqs(mdhim_t *md); //Clean outstanding reqs

#endif
