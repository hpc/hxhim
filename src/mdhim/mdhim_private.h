#ifndef MDHIM_PRIVATE_H
#define MDHIM_PRIVATE_H

#include "mdhim.h"
#include "mdhim_options_private.h"
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
    // Actual transport layer
    Transport *transport;

    // the function that runs as the transport listener thread
    void *(*listener_thread)(void *);

    // the function called once data has been processed by the range server
    int (*send_client_response)(work_item_t *, TransportMessage *, volatile int &);

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
    TransportMessage *receive_msg;

    //Options for DB creation
    mdhim_db_options_t *db_opts;
} mdhim_private_t;

/** @description The actual initializer function for mdhim_t */
int mdhim_private_init(mdhim_t *md, mdhim_db_options_t *db, mdhim_transport_options_t *transport);

/** @description The actual destructor for mdhim_t */
int mdhim_private_destroy(mdhim_t *md);

/** @description Internal PUT function */
TransportRecvMessage *_put_record(mdhim_t *md, index_t *index,
                                  void *key, int key_len,
                                  void *value, int value_len);

/** @description Internal GET function */
TransportGetRecvMessage *_get_record(mdhim_t *md, index_t *index,
                                     void *key, int key_len,
                                     enum TransportGetMessageOp op);

/** @description Creates Bulk Receive Messages from Receive Messages to be used as a list */
TransportBRecvMessage *_create_brm(TransportRecvMessage *rm);

/** @description Adds a Bulk Recieve Message to the end of a Bulk Receive Message list*/
void _concat_brm(TransportBRecvMessage *head, TransportBRecvMessage *addition);

/** @description Internal BPUT function */
TransportBRecvMessage *_bput_records(mdhim_t *md, index_t *index,
                                     void **keys, int *key_lens,
                                     void **values, int *value_lens,
                                     int num_records);

/** @description Internal BGET function */
TransportBGetRecvMessage *_bget_records(mdhim_t *md, index_t *index,
                                        void **keys, int *key_lens,
                                        int num_keys, int num_records,
                                        enum TransportGetMessageOp op);

/** @description Internal BDELETE function */
TransportBRecvMessage *_bdel_records(mdhim_t *md, index_t *index,
                                     void **keys, int *key_lens,
                                     int num_records);

/** @description Internal function for getting the destination range server of a key */
int _which_server(mdhim_t *md, void *key, int key_len);

#ifdef __cplusplus
}
#endif

#endif
