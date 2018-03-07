/*
 * MDHIM TNG
 *
 * Client specific implementation
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "range_server.h"
#include "partitioner.h"
#include "mdhim_options.h"
#include "mdhim_private.h"

static void add_timing(struct timeval start, struct timeval end, int num,
        struct mdhim *md, int mtype) {
    long double elapsed;

    elapsed = (long double) (end.tv_sec - start.tv_sec) +
        ((long double) (end.tv_usec - start.tv_usec)/1000000.0);
    if (mtype == MDHIM_PUT || mtype == MDHIM_BULK_PUT) {
        md->p->mdhim_rs->put_time += elapsed;
        md->p->mdhim_rs->num_put += num;
    } else if (mtype == MDHIM_BULK_GET) {
        md->p->mdhim_rs->get_time += elapsed;
        md->p->mdhim_rs->num_get += num;
    }
}

/**
 * send_locally_or_remote
 * Sends the message remotely or locally
 *
 * @param md       Pointer to the main MDHIM structure
 * @param dest     Destination rank
 * @param message  pointer to message to send
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int send_locally_or_remote(struct mdhim *md, int dest, TransportMessage *message) {
    int ret = MDHIM_SUCCESS;
    if ((int) *md->p->transport->Endpoint()->Address() != dest) {
        //Sends the message remotely
        MPI_Request **size_req = new MPI_Request*();
        MPI_Request **msg_req = new MPI_Request*();
        void **sendbuf = new void *();
        int *sizebuf = new int();

        ret = send_client_response(md, dest, message, sizebuf,
                                   sendbuf, size_req, msg_req);
        if (*size_req) {
            range_server_add_oreq(md, *size_req, sizebuf);
        }

        if (*msg_req) {
            range_server_add_oreq(md, *msg_req, *sendbuf);
        } else if (*sendbuf) {
            free(*sendbuf);
        }

        delete sendbuf;
        delete message;
        delete size_req;
        delete msg_req;
    } else {
        //Sends the message locally
        pthread_mutex_lock(&md->p->receive_msg_mutex);
        md->p->receive_msg = message;
        pthread_cond_signal(&md->p->receive_msg_ready_cv);
        pthread_mutex_unlock(&md->p->receive_msg_mutex);
    }

    return ret;
}

static index_t *find_index(struct mdhim *md, TransportMessage *msg) {
    return get_index(md, msg->index);
}

/*
 * ===  FUNCTION  ======================================================================
 *         Name:  find_index_by_name
 *  Description:  Search for index by name
 *    Variables:  <struct mdhim_t *md> the pointer to the mdhim structure
 *                <struct mdhim_basem_t *msg> A pointer to a base message that contains
 *                                            the name of the index
 * =====================================================================================
 */
static index_t * find_index_by_name(struct mdhim *md, TransportMessage *msg) {
    return get_index_by_name(md, msg->index_name);
}

/**
 * range_server_add_work
 * Adds work to the work queue and signals the condition variable for the worker thread
 *
 * @param md      Pointer to the main MDHIM structure
 * @param item    pointer to new work item that contains a message to handle
 * @return MDHIM_SUCCESS
 */
int range_server_add_work(struct mdhim *md, work_item_t *item) {
    //Lock the work queue mutex
    pthread_mutex_lock(md->p->mdhim_rs->work_queue_mutex);
    item->next = NULL;
    item->prev = NULL;

    //Add work to the tail of the work queue
    if (md->p->mdhim_rs->work_queue->tail) {
        md->p->mdhim_rs->work_queue->tail->next = item;
        item->prev = md->p->mdhim_rs->work_queue->tail;
        md->p->mdhim_rs->work_queue->tail = item;
    } else {
        md->p->mdhim_rs->work_queue->head = item;
        md->p->mdhim_rs->work_queue->tail = item;
    }

    //Signal the waiting thread that there is work available
    pthread_cond_signal(md->p->mdhim_rs->work_ready_cv);
    pthread_mutex_unlock(md->p->mdhim_rs->work_queue_mutex);

    return MDHIM_SUCCESS;
}

/**
 * get_work
 * Returns the next work item from the work queue
 *
 * @param md  Pointer to the main MDHIM structure
 * @return  the next work_item to process
 */

static work_item_t *get_work(struct mdhim *md) {
    work_item_t *item = md->p->mdhim_rs->work_queue->head;
    if (!item) {
        return NULL;
    }

    //Set the list head and tail to NULL
    md->p->mdhim_rs->work_queue->head = NULL;
    md->p->mdhim_rs->work_queue->tail = NULL;

    //Return the list
    return item;
}

/**
 * range_server_stop
 * Stop the range server (i.e., stops the threads and frees the relevant data in md)
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_stop(struct mdhim *md) {
    int ret;
    work_item *head, *temp_item;

    //Signal to the listener thread that it needs to shutdown
    md->p->shutdown = 1;

    /* Wait for the threads to finish */
    pthread_mutex_lock(md->p->mdhim_rs->work_queue_mutex);
    pthread_cond_broadcast(md->p->mdhim_rs->work_ready_cv);
    pthread_mutex_unlock(md->p->mdhim_rs->work_queue_mutex);

    pthread_join(md->p->mdhim_rs->listener, NULL);
    /* Wait for the threads to finish */
    for (int i = 0; i < md->p->db_opts->num_wthreads; i++) {
        pthread_join(*md->p->mdhim_rs->workers[i], NULL);
        free(md->p->mdhim_rs->workers[i]);
    }
    free(md->p->mdhim_rs->workers);

    //Destroy the condition variables
    if ((ret = pthread_cond_destroy(md->p->mdhim_rs->work_ready_cv)) != 0) {
      mlog(MDHIM_SERVER_DBG, "Rank %s - Error destroying work cond variable",
           ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
    }
    free(md->p->mdhim_rs->work_ready_cv);

    //Destroy the work queue mutex
    if ((ret = pthread_mutex_destroy(md->p->mdhim_rs->work_queue_mutex)) != 0) {
      mlog(MDHIM_SERVER_DBG, "Rank %s - Error destroying work queue mutex",
           ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
    }
    free(md->p->mdhim_rs->work_queue_mutex);

    //Clean outstanding sends
    range_server_clean_oreqs(md);
    //Destroy the out req mutex
    if ((ret = pthread_mutex_destroy(md->p->mdhim_rs->out_req_mutex)) != 0) {
      mlog(MDHIM_SERVER_DBG, "Rank %s - Error destroying work queue mutex",
           ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
    }
    free(md->p->mdhim_rs->out_req_mutex);

    //Free the work queue
    head = md->p->mdhim_rs->work_queue->head;
    while (head) {
      temp_item = head->next;
      free(head);
      head = temp_item;
    }
    free(md->p->mdhim_rs->work_queue);

    mlog(MDHIM_SERVER_INFO, "Rank %s - Inserted: %ld records in %Lf seconds",
         ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), md->p->mdhim_rs->num_put, md->p->mdhim_rs->put_time);
    mlog(MDHIM_SERVER_INFO, "Rank %s - Retrieved: %ld records in %Lf seconds",
         ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), md->p->mdhim_rs->num_get, md->p->mdhim_rs->get_time);

    //Free the range server data
    free(md->p->mdhim_rs);
    md->p->mdhim_rs = NULL;

    return MDHIM_SUCCESS;
}

/**
 * range_server_put
 * Handles the put message and puts data in the database
 *
 * @param md        pointer to the main MDHIM struct
 * @param im        pointer to the put message to handle
 * @param source    source of the message
 * @return          MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int range_server_put(struct mdhim *md, TransportPutMessage *im, int source) {
    int ret;
    int error = 0;
    void **value;
    int32_t *value_len;
    int exists = 0;
    void *new_value;
    int32_t new_value_len;
    void *old_value;
    int32_t old_value_len;
    struct timeval start, end;
    int inserted = 0;
    index_t *index;

    value = (void**)malloc(sizeof(void *));
    *value = NULL;
    value_len = (int32_t*)malloc(sizeof(int32_t));
    *value_len = 0;

    //Get the index referenced the message
    index = find_index(md, (TransportMessage *) im);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %s - Error retrieving index for id: %d",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), im->index);
        error = MDHIM_ERROR;
        goto done;
    }

    gettimeofday(&start, NULL);
       //Check for the key's existence
/*    index->mdhim_store->get(index->mdhim_store->db_handle,
                       im->key, im->key_len, value,
                       value_len);
*/
    //The key already exists
    if (*value && *value_len) {
        exists = 1;
    }

        //If the option to append was specified and there is old data, concat the old and new
    if (exists &&  md->p->db_opts->db_value_append == MDHIM_DB_APPEND) {
        old_value = *value;
        old_value_len = *value_len;
        new_value_len = old_value_len + im->value_len;
        new_value = malloc(new_value_len);
        memcpy(new_value, old_value, old_value_len);
        memcpy((char*)new_value + old_value_len, im->value, im->value_len);
    } else {
        new_value = im->value;
        new_value_len = im->value_len;
    }

    if (*value && *value_len) {
        free(*value);
    }
    free(value);
    free(value_len);
        //Put the record in the database
    if ((ret =
         index->mdhim_store->put(index->mdhim_store->db_handle,
                     im->key, im->key_len, new_value,
                     new_value_len)) != MDHIM_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "Rank %s - Error putting record",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        error = ret;
    } else {
        inserted = 1;
    }

    if (!exists && error == MDHIM_SUCCESS) {
        update_stat(md, index, im->key, im->key_len);
    }

    gettimeofday(&end, NULL);
    add_timing(start, end, inserted, md, MDHIM_PUT);

done:
    //Create the response message
    TransportRecvMessage *rm = new TransportRecvMessage();
    //Set the type
    rm->mtype = TransportMessageType::RECV;
    //Set the operation return code as the error
    rm->error = error;
    //Set the server's rank
    rm->server_rank = (int) *md->p->transport->Endpoint()->Address();

    //Send response
    ret = send_locally_or_remote(md, source, static_cast<TransportMessage *>(rm));

    //Free memory
    if (exists && md->p->db_opts->db_value_append == MDHIM_DB_APPEND) {
        free(new_value);
    }
    if (source != (int) *md->p->transport->Endpoint()->Address()) {
        // free(im->key);
        // free(im->value);
    }
    // TODO: put this back in - it is currently freeing something it shouldnt be freeing
    // free(im);

    return MDHIM_SUCCESS;
}

/**
 * range_server_bput
 * Handles the bulk put message and puts data in the database
 *
 * @param md        Pointer to the main MDHIM struct
 * @param bim       pointer to the bulk put message to handle
 * @param source    source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int range_server_bput(struct mdhim *md, TransportBPutMessage *bim, int source) {
    int ret;
    int error = MDHIM_SUCCESS;
    void **value;
    int32_t value_len;
    void *new_value;
    int32_t new_value_len;
    void *old_value;
    int32_t old_value_len;
    struct timeval start, end;
    int num_put = 0;
    index_t *index;

    gettimeofday(&start, NULL);

    int *exists = new int[bim->num_keys]();
    void **new_values = (void**)malloc(bim->num_keys * sizeof(void *));
    int32_t *new_value_lens = new int32_t[bim->num_keys]();
    value = (void**)malloc(sizeof(void *));

    //Get the index referenced the message
    index = find_index(md, static_cast<TransportMessage *>(bim));
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %s - Error retrieving index for id: %d",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), bim->index);
        error = MDHIM_ERROR;
        goto done;
    }

    //Iterate through the arrays and insert each record
    for (int i = 0; i < bim->num_keys && i < MAX_BULK_OPS; i++) {
        *value = NULL;
        value_len = 0;

        //Check for the key's existence
        index->mdhim_store->get(index->mdhim_store->db_handle,
                                bim->keys[i], bim->key_lens[i], value,
                                &value_len);
        //The key already exists
        if (*value && value_len) {
            exists[i] = 1;
        } else {
            exists[i] = 0;
        }

        //If the option to append was specified and there is old data, concat the old and new
        if (exists[i] && md->p->db_opts->db_value_append == MDHIM_DB_APPEND) {
            old_value = *value;
            old_value_len = value_len;
            new_value_len = old_value_len + bim->value_lens[i];
            new_value = malloc(new_value_len);
            memcpy(new_value, old_value, old_value_len);
            memcpy((char*)new_value + old_value_len, bim->values[i], bim->value_lens[i]);
            if (exists[i] && source != (int) *md->p->transport->Endpoint()->Address()) {
                free(bim->values[i]);
            }

            new_values[i] = new_value;
            new_value_lens[i] = new_value_len;
        } else {
            new_values[i] = bim->values[i];
            new_value_lens[i] = bim->value_lens[i];
        }

        if (*value) {
            free(*value);
        }
    }

    //Put the record in the database
    if ((ret =
         index->mdhim_store->batch_put(index->mdhim_store->db_handle,
                       bim->keys, bim->key_lens, new_values,
                       new_value_lens, bim->num_keys)) != MDHIM_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "Rank %s - Error batch putting records",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        error = ret;
    } else {
        num_put = bim->num_keys;
    }

    for (int i = 0; i < bim->num_keys && i < MAX_BULK_OPS; i++) {
        //Update the stats if this key didn't exist before
        if (!exists[i] && error == MDHIM_SUCCESS) {
            update_stat(md, index, bim->keys[i], bim->key_lens[i]);
        }

        if (exists[i] && md->p->db_opts->db_value_append == MDHIM_DB_APPEND) {
            //Release the value created for appending the new and old value
            free(new_values[i]);
        }

        //Release the bput keys/value if the message isn't coming from myself
        if (source != (int) *md->p->transport->Endpoint()->Address()) {
            free(bim->keys[i]);
            free(bim->values[i]);
        }
    }

    delete [] exists;
    free(new_values);
    free(new_value_lens);
    free(value);
    gettimeofday(&end, NULL);
    add_timing(start, end, num_put, md, MDHIM_BULK_PUT);

 done:
    //Create the response message
    TransportBRecvMessage *brm = new TransportBRecvMessage();
    //Set the type
    brm->mtype = TransportMessageType::RECV;
    //Set the operation return code as the error
    brm->error = error;
    //Set the server's rank
    brm->server_rank = (int) *md->p->transport->Endpoint()->Address();

    //Release the internals of the bput message
    // TODO: put this back in
    // delete bim;

    //Send response
    ret = send_locally_or_remote(md, source, brm);

    return MDHIM_SUCCESS;
}

// /**
//  * range_server_del
//  * Handles the delete message and deletes the data from the database
//  *
//  * @param md       Pointer to the main MDHIM struct
//  * @param dm       pointer to the delete message to handle
//  * @param source   source of the message
//  * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
//  */
// static int range_server_del(struct mdhim *md, struct mdhim_delm_t *dm, int source) {
//     int ret = MDHIM_ERROR;
//     struct mdhim_rm_t *rm;
//     index_t *index;

//     //Get the index referenced the message
//     index = find_index(md, (struct mdhim_basem_t *) dm);
//     if (!index) {
//         mlog(MDHIM_SERVER_CRIT, "Rank %s - Error retrieving index for id: %d",
//              ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), dm->basem.index);
//         ret = MDHIM_ERROR;
//         goto done;
//     }

//     //Put the record in the database
//     if ((ret =
//          index->mdhim_store->del(index->mdhim_store->db_handle,
//                      dm->key, dm->key_len)) != MDHIM_SUCCESS) {
//         mlog(MDHIM_SERVER_CRIT, "Rank %s - Error deleting record",
//              ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
//     }

//  done:
//     //Create the response message
//     rm = (mdhim_rm_t*)malloc(sizeof(struct mdhim_rm_t));
//     //Set the type
//     rm->basem.mtype = MDHIM_RECV;
//     //Set the operation return code as the error
//     rm->error = ret;
//     //Set the server's rank
//     rm->basem.server_rank = (int) *md->p->transport->Endpoint()->Address();

//     //Send response
//     ret = send_locally_or_remote(md, source, rm);
//     free(dm);

//     return MDHIM_SUCCESS;
// }

// /**
//  * range_server_bdel
//  * Handles the bulk delete message and deletes the data from the database
//  *
//  * @param md        Pointer to the main MDHIM struct
//  * @param bdm       pointer to the bulk delete message to handle
//  * @param source    source of the message
//  * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
//  */
// int range_server_bdel(struct mdhim *md, struct mdhim_bdelm_t *bdm, int source) {
//      int i;
//     int ret;
//     int error = 0;
//     struct mdhim_rm_t *brm;
//     index_t *index;

//     //Get the index referenced the message
//     index = find_index(md, (struct mdhim_basem_t *) bdm);
//     if (!index) {
//         mlog(MDHIM_SERVER_CRIT, "Rank %s - Error retrieving index for id: %d",
//              ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), bdm->basem.index);
//         error = MDHIM_ERROR;
//         goto done;
//     }

//     //Iterate through the arrays and delete each record
//     for (i = 0; i < bdm->num_keys && i < MAX_BULK_OPS; i++) {
//         //Put the record in the database
//         if ((ret =
//              index->mdhim_store->del(index->mdhim_store->db_handle,
//                          bdm->keys[i], bdm->key_lens[i]))
//             != MDHIM_SUCCESS) {
//             mlog(MDHIM_SERVER_CRIT, "Rank %s - Error deleting record",
//                  ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
//             error = ret;
//         }
//     }

// done:
//     //Create the response message
//     brm = (mdhim_rm_t*)malloc(sizeof(struct mdhim_rm_t));
//     //Set the type
//     brm->basem.mtype = MDHIM_RECV;
//     //Set the operation return code as the error
//     brm->error = error;
//     //Set the server's rank
//     brm->basem.server_rank = (int) *md->p->transport->Endpoint()->Address();

//     //Send response
//     ret = send_locally_or_remote(md, source, brm);
//     free(bdm->keys);
//     free(bdm->key_lens);
//     free(bdm);

//     return MDHIM_SUCCESS;
// }

/**
 * range_server_commit
 * Handles the commit message and commits outstanding writes to the database
 *
 * @param md        pointer to the main MDHIM struct
 * @param im        pointer to the commit message to handle
 * @param source    source of the message
 * @return          MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int range_server_commit(struct mdhim *md, TransportMessage *im, int source) {
    int ret;
    index_t *index;

    //Get the index referenced the message
    index = find_index(md, im);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %s - Error retrieving index for id: %d",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), im->index);
        ret = MDHIM_ERROR;
        goto done;
    }

        //Put the record in the database
    if ((ret =
         index->mdhim_store->commit(index->mdhim_store->db_handle))
        != MDHIM_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "Rank %s - Error committing database",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
    }

 done:
    //Create the response message
    TransportRecvMessage *rm = new TransportRecvMessage();
    //Set the type
    rm->mtype = TransportMessageType::RECV;
    //Set the operation return code as the error
    rm->error = ret;
    //Set the server's rank
    rm->server_rank = (int) *md->p->transport->Endpoint()->Address();

    //Send response
    ret = send_locally_or_remote(md, source, rm);
    // free(im);
    delete im;

    return MDHIM_SUCCESS;
}

/**
 * range_server_bget
 * Handles the bulk get message, retrieves the data from the database, and sends the results back
 *
 * @param md        Pointer to the main MDHIM struct
 * @param bgm       pointer to the bulk get message to handle
 * @param source    source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int range_server_bget(struct mdhim *md, TransportBGetMessage *bgm, int source) {
    int ret;
    void **values;
    int32_t *value_lens = (int32_t *)calloc(bgm->num_keys, sizeof(int32_t));
    TransportBGetRecvMessage *bgrm;
    int error = 0;
    struct timeval start, end;
    int num_retrieved = 0;
    index_t *index;

    gettimeofday(&start, NULL);
    values = (void**)malloc(sizeof(void *) * bgm->num_keys);

    //Get the index referenced the message
    index = find_index(md, (TransportMessage *) bgm);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %s - Error retrieving index for id: %d",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), bgm->index);
        error = MDHIM_ERROR;
        goto done;
    }

    //Iterate through the arrays and get each record
    for (int i = 0; i < bgm->num_keys && i < MAX_BULK_OPS; i++) {
        switch(bgm->op) {
            // Gets the value for the given key
            case TransportGetMessageOp::GET_EQ:
                //Get records from the database
                if ((ret =
                     index->mdhim_store->get(index->mdhim_store->db_handle,
                                             bgm->keys[i], bgm->key_lens[i], &values[i],
                                             &value_lens[i])) != MDHIM_SUCCESS) {
                    error = ret;
                    value_lens[i] = 0;
                    values[i] = NULL;
                    continue;
                }
                break;
            /* Gets the next key and value that is in order after the passed in key */
            case TransportGetMessageOp::GET_NEXT:
                if ((ret =
                     index->mdhim_store->get_next(index->mdhim_store->db_handle,
                                                  &bgm->keys[i], &bgm->key_lens[i], &values[i],
                                                  &value_lens[i])) != MDHIM_SUCCESS) {
                    mlog(MDHIM_SERVER_DBG, "Rank %s - Error getting record", ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
                    error = ret;
                    value_lens[i] = 0;
                    values[i] = NULL;
                    continue;
                }

                break;
            /* Gets the previous key and value that is in order before the passed in key
               or the last key if no key was passed in */
            case TransportGetMessageOp::GET_PREV:
                if ((ret =
                     index->mdhim_store->get_prev(index->mdhim_store->db_handle,
                                                  &bgm->keys[i], &bgm->key_lens[i], &values[i],
                                                  &value_lens[i])) != MDHIM_SUCCESS) {
                    mlog(MDHIM_SERVER_DBG, "Rank %s - Error getting record", ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
                    error = ret;
                    value_lens[i] = 0;
                    values[i] = NULL;
                    continue;
                }

                break;
             /* Gets the first key/value */
            case TransportGetMessageOp::GET_FIRST:
                if ((ret =
                     index->mdhim_store->get_next(index->mdhim_store->db_handle,
                                                  &bgm->keys[i], 0, &values[i],
                                                  &value_lens[i])) != MDHIM_SUCCESS) {
                    mlog(MDHIM_SERVER_DBG, "Rank %s - Error getting record", ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
                    error = ret;
                    value_lens[i] = 0;
                    values[i] = NULL;
                    continue;
                }

                break;
            /* Gets the last key/value */
            case TransportGetMessageOp::GET_LAST:
                if ((ret =
                     index->mdhim_store->get_prev(index->mdhim_store->db_handle,
                                                  &bgm->keys[i], 0, &values[i],
                                                  &value_lens[i])) != MDHIM_SUCCESS) {
                    mlog(MDHIM_SERVER_DBG, "Rank %s - Error getting record", ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
                    error = ret;
                    value_lens[i] = 0;
                    values[i] = NULL;
                    continue;
                }

                break;
            default:
                mlog(MDHIM_SERVER_DBG, "Rank %s - Invalid operation: %d given in range_server_get",
                     ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), (int) bgm->op);
                continue;
        }

        num_retrieved++;
    }

    gettimeofday(&end, NULL);
    add_timing(start, end, num_retrieved, md, MDHIM_BULK_GET);

done:
    //Create the response message
    bgrm = new TransportBGetRecvMessage();
    //Set the type
    bgrm->mtype = TransportMessageType::RECV_BGET;
    //Set the operation return code as the error
    bgrm->error = error;
    //Set the server's rank
    bgrm->server_rank = (int) *md->p->transport->Endpoint()->Address();
    //Set the key and value
    if (source == (int) *md->p->transport->Endpoint()->Address()) {
        //If this message is coming from myself, copy the keys
        bgrm->key_lens = (int*)malloc(bgm->num_keys * sizeof(int));
        bgrm->keys = (void**)malloc(bgm->num_keys * sizeof(void *));
        for (int i = 0; i < bgm->num_keys; i++) {
            bgrm->key_lens[i] = bgm->key_lens[i];
            bgrm->keys[i] = malloc(bgrm->key_lens[i]);
            memcpy(bgrm->keys[i], bgm->keys[i], bgrm->key_lens[i]);
        }

        free(bgm->keys);
        free(bgm->key_lens);
    } else {
        bgrm->keys = bgm->keys;
        bgrm->key_lens = bgm->key_lens;
    }

    bgrm->values = values;
    bgrm->value_lens = value_lens;
    bgrm->num_keys = bgm->num_keys;
    bgrm->index = index->id;
    bgrm->index_type = index->type;

    //Send response
    ret = send_locally_or_remote(md, source, bgrm);

    //Release the bget message
    // delete bgm;

    return MDHIM_SUCCESS;
}

/**
 * range_server_bget_op
 * Handles the get message given an op and number of records greater than 1
 *
 * @param md        Pointer to the main MDHIM struct
 * @param gm        pointer to the get message to handle
 * @param source    source of the message
 * @param op        operation to perform
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int range_server_bget_op(struct mdhim *md, TransportBGetMessage *bgm, int source, TransportGetMessageOp op) {
    int error = 0;
    void **values;
    void **keys;
    void **get_key; //Used for passing the key to the db
    int *get_key_len; //Used for passing the key len to the db
    void **get_value;
    int *get_value_len;
    int32_t *key_lens;
    int32_t *value_lens;
    TransportBGetRecvMessage *bgrm;
    int ret;
    int i, j;
    int num_records;
    struct timeval start, end;
    index_t *index;

    //Initialize pointers and lengths
    values = (void**)malloc(sizeof(void *) * bgm->num_keys * bgm->num_recs);
    value_lens = (int*)malloc(sizeof(int32_t) * bgm->num_keys * bgm->num_recs);
    memset(value_lens, 0, sizeof(int32_t) *bgm->num_keys * bgm->num_recs);
    keys = (void**)malloc(sizeof(void *) * bgm->num_keys * bgm->num_recs);
    memset(keys, 0, sizeof(void *) * bgm->num_keys * bgm->num_recs);
    key_lens = (int*)malloc(sizeof(int32_t) * bgm->num_keys * bgm->num_recs);
    memset(key_lens, 0, sizeof(int32_t) * bgm->num_keys * bgm->num_recs);
    get_key = (void**)malloc(sizeof(void *));
    *get_key = NULL;
    get_key_len = (int*)malloc(sizeof(int32_t));
    *get_key_len = 0;
    get_value = (void**)malloc(sizeof(void *));
    get_value_len = (int*)malloc(sizeof(int32_t));
    num_records = 0;

    //Get the index referenced the message
    index = find_index(md, (TransportMessage *) bgm);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %s - Error retrieving index for id: %d",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), bgm->index);
        error = MDHIM_ERROR;
        goto respond;
    }

    if (bgm->num_keys * bgm->num_recs > MAX_BULK_OPS) {
        mlog(MDHIM_SERVER_CRIT, "Rank %s - Too many bulk operations requested",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        error = MDHIM_ERROR;
        goto respond;
    }

    mlog(MDHIM_SERVER_CRIT, "Rank %s - Num keys is: %d and num recs is: %d",
         ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), bgm->num_keys, bgm->num_recs);
    gettimeofday(&start, NULL);
    //Iterate through the arrays and get each record
    for (i = 0; i < bgm->num_keys; i++) {
        for (j = 0; j < bgm->num_recs; j++) {
            keys[num_records] = NULL;
            key_lens[num_records] = 0;

            //If we were passed in a key, copy it
            if (!j && bgm->key_lens[i] && bgm->keys[i]) {
                *get_key = malloc(bgm->key_lens[i]);
                memcpy(*get_key, bgm->keys[i], bgm->key_lens[i]);
                *get_key_len = bgm->key_lens[i];
                //If we were not passed a key and this is a next/prev, then return an error
            } else if (!j && (!bgm->key_lens[i] || !bgm->keys[i])
                   && (op ==  TransportGetMessageOp::GET_NEXT ||
                       op == TransportGetMessageOp::GET_PREV)) {
                error = MDHIM_ERROR;
                goto respond;
            }

            switch(op) {
                //Get a record from the database
            case TransportGetMessageOp::GET_FIRST:
                if (j == 0) {
                    keys[num_records] = NULL;
                    key_lens[num_records] = sizeof(int32_t);
                }
            case TransportGetMessageOp::GET_NEXT:
                if (j && (ret =
                      index->mdhim_store->get_next(index->mdhim_store->db_handle,
                                       get_key, get_key_len,
                                       get_value,
                                       get_value_len))
                    != MDHIM_SUCCESS) {
                    mlog(MDHIM_SERVER_DBG, "Rank %s - Couldn't get next record",
                         ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
                    error = ret;
                    key_lens[num_records] = 0;
                    value_lens[num_records] = 0;
                    goto respond;
                } else if (!j && (ret =
                          index->mdhim_store->get(index->mdhim_store->db_handle,
                                      *get_key, *get_key_len,
                                      get_value,
                                      get_value_len))
                       != MDHIM_SUCCESS) {
                    error = ret;
                    key_lens[num_records] = 0;
                    value_lens[num_records] = 0;
                    goto respond;
                }
                break;
            case TransportGetMessageOp::GET_LAST:
                if (j == 0) {
                    keys[num_records] = NULL;
                    key_lens[num_records] = sizeof(int32_t);
                }
            case TransportGetMessageOp::GET_PREV:
                if (j && (ret =
                      index->mdhim_store->get_prev(index->mdhim_store->db_handle,
                                       get_key, get_key_len,
                                       get_value,
                                       get_value_len))
                    != MDHIM_SUCCESS) {
                    mlog(MDHIM_SERVER_DBG, "Rank %s - Couldn't get prev record",
                         ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
                    error = ret;
                    key_lens[num_records] = 0;
                    value_lens[num_records] = 0;
                    goto respond;
                } else if (!j && (ret =
                          index->mdhim_store->get(index->mdhim_store->db_handle,
                                      *get_key, *get_key_len,
                                      get_value,
                                      get_value_len))
                       != MDHIM_SUCCESS) {
                    error = ret;
                    key_lens[num_records] = 0;
                    value_lens[num_records] = 0;
                    goto respond;
                }
                break;
            default:
                mlog(MDHIM_SERVER_CRIT, "Rank %s - Invalid operation for bulk get op",
                     ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
                goto respond;
                break;
            }

            keys[num_records] = *get_key;
            key_lens[num_records] = *get_key_len;
            values[num_records] = *get_value;
            value_lens[num_records] = *get_value_len;
            num_records++;
        }
    }

respond:

    gettimeofday(&end, NULL);
    add_timing(start, end, num_records, md, MDHIM_BULK_GET);

    //Create the response message
    bgrm = new TransportBGetRecvMessage();
    //Set the type
    bgrm->mtype = TransportMessageType::RECV_BGET;
    //Set the operation return code as the error
    bgrm->error = error;
    //Set the server's rank
    bgrm->server_rank = (int) *md->p->transport->Endpoint()->Address();
    //Set the keys and values
    bgrm->keys = keys;
    bgrm->key_lens = key_lens;
    bgrm->values = values;
    bgrm->value_lens = value_lens;
    bgrm->num_keys = num_records;
    bgrm->index = index->id;
    bgrm->index_type = index->type;

    //Send response
    ret = send_locally_or_remote(md, source, bgrm);

    //Free stuff
    if (source == (int) *md->p->transport->Endpoint()->Address()) {
        /* If this message is not coming from myself,
           free the keys and values from the get message */
        // mdhim_partial_release_msg(bgm);
        delete bgm;
    }

    free(get_key);
    free(get_key_len);
    free(get_value);
    free(get_value_len);

    return MDHIM_SUCCESS;
}

/*
 * listener_thread
 * Function for the thread that listens for new messages
 */
static void *listener_thread(void *data) {
    //Mlog statements could cause a deadlock on range_server_stop due to canceling of threads

    struct mdhim *md = (struct mdhim *) data;
    TransportMessage *message = nullptr;
    int source; //The source of the message
    int ret;
    work_item *item;

    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

    while (1) {
        // printf("Rank %s - Listener thread waiting\n",
        //        ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());

        if (md->p->shutdown) {
            break;
        }

        //Clean outstanding sends
        range_server_clean_oreqs(md);

        //Receive messages sent to this server
        ret = receive_rangesrv_work(md, &source, &message);
        if (ret < MDHIM_SUCCESS) {
            // printf("Rank %s - Listener thread failed to receive\n",
            //        ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
            continue;
        }

        // printf("Rank %s - Listener thread received message from rank: %d of type: %d",
        //        ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), source, (int) message->mtype);

        //Create a new work item
        item = (work_item*)malloc(sizeof(work_item));
        memset(item, 0, sizeof(work_item));

        //Set the new buffer to the new item's message
        item->message = message;
        //Set the source in the work item
        item->source = source;
        //Add the new item to the work queue
        range_server_add_work(md, item);
    }

    return NULL;
}

/*
 * worker_thread
 * Function for the thread that processes work in work queue
 */
static void *worker_thread(void *data) {
    //Mlog statements could cause a deadlock on range_server_stop due to canceling of threads
    mdhim_t *md = (mdhim_t *) data;
    work_item *item = nullptr;

    while (true) {
        if (md->p->shutdown) {
            break;
        }
        //Lock the work queue mutex
        pthread_mutex_lock(md->p->mdhim_rs->work_queue_mutex);
        pthread_cleanup_push((void (*)(void *)) pthread_mutex_unlock,
                             (void *) md->p->mdhim_rs->work_queue_mutex);

        //Wait until there is work to be performed
        //Do not loop
        if ((item = get_work(md)) == nullptr) {
            pthread_cond_wait(md->p->mdhim_rs->work_ready_cv, md->p->mdhim_rs->work_queue_mutex);
            item = get_work(md);
        }

        pthread_cleanup_pop(0);
        pthread_mutex_unlock(md->p->mdhim_rs->work_queue_mutex);

        if (!item) {
            continue;
        }

        //Clean outstanding sends
        range_server_clean_oreqs(md);

        while (item) {
            TransportMessage *message = static_cast<TransportMessage *>(item->message);

            //Call the appropriate function depending on the message type
            switch(message->mtype) {
            case TransportMessageType::PUT:
                //Pack the put message and pass to range_server_put
                range_server_put(md,
                                 dynamic_cast<TransportPutMessage *>(message),
                                 item->source);
                break;
            case TransportMessageType::BPUT:
                //Pack the bulk put message and pass to range_server_put
                range_server_bput(md,
                                  dynamic_cast<TransportBPutMessage *>(message),
                                  item->source);
                break;
            case TransportMessageType::BGET:
                {
                    TransportBGetMessage *bgm = dynamic_cast<TransportBGetMessage *>(message);
                    //The client is sending one key, but requesting the retrieval of more than one
                    if (bgm->num_recs > 1 && bgm->num_keys == 1) {
                        range_server_bget_op(md, bgm, item->source, bgm->op);
                    }
                    else {
                        range_server_bget(md, bgm, item->source);
                    }
                }
                break;
            case TransportMessageType::DELETE:
                // range_server_del(md, dynamic_cast<TransportDeleteMessage *>(item->message), item->source);
                break;
            case TransportMessageType::BDELETE:
                // range_server_del(md, dynamic_cast<TransportBDeleteMessage *>(item->message), item->source);
                break;
            case TransportMessageType::COMMIT:
                range_server_commit(md, static_cast<TransportMessage *>(item->message), item->source);
                break;
            default:
                break;
            }

            work_item *item_tmp = item;
            item = item->next;
            delete item_tmp;
        }

        //Clean outstanding sends
        range_server_clean_oreqs(md);
    }

    return NULL;
}

int range_server_add_oreq(struct mdhim *md, MPI_Request *req, void *msg) {
    out_req *oreq;
    out_req *item;

    pthread_mutex_lock(md->p->mdhim_rs->out_req_mutex);
    item = md->p->mdhim_rs->out_req_list;
    oreq = (out_req*)malloc(sizeof(out_req));
    oreq->next = NULL;
    oreq->prev = NULL;
    oreq->message = (MPI_Request*)msg;
    oreq->req = req;

    if (!item) {
        md->p->mdhim_rs->out_req_list = oreq;
        pthread_mutex_unlock(md->p->mdhim_rs->out_req_mutex);
        return MDHIM_SUCCESS;
    }

    item->prev = oreq;
    oreq->next = item;
    md->p->mdhim_rs->out_req_list = oreq;
    pthread_mutex_unlock(md->p->mdhim_rs->out_req_mutex);

    return MDHIM_SUCCESS;
}

int range_server_clean_oreqs(struct mdhim *md) {
    pthread_mutex_lock(md->p->mdhim_rs->out_req_mutex);
    out_req *item = md->p->mdhim_rs->out_req_list;
    while (item) {
        if (!item->req) {
            item = item->next;
            continue;
        }

        pthread_mutex_lock(&md->p->mdhim_comm_lock);
        int flag = 0;
        MPI_Status status;
        MPI_Test((MPI_Request *)item->req, &flag, &status);
        pthread_mutex_unlock(&md->p->mdhim_comm_lock);

        if (!flag) {
            item = item->next;
            continue;
        }

        if (item == md->p->mdhim_rs->out_req_list) {
            md->p->mdhim_rs->out_req_list = item->next;
            if (item->next) {
                item->next->prev = NULL;
            }
        } else {
            if (item->next) {
                item->next->prev = item->prev;
            }
            if (item->prev) {
                item->prev->next = item->next;
            }
        }

        out_req *t = item->next;
        free(item->req);
        if (item->message) {
            free(item->message);
        }

        free(item);
        item = t;
    }

    pthread_mutex_unlock(md->p->mdhim_rs->out_req_mutex);

    return MDHIM_SUCCESS;
}

/**
 * range_server_init
 * Initializes the range server (i.e., starts the threads and populates the relevant data in md)
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_init(struct mdhim *md) {
    int ret;
    int i;

    //Allocate memory for the mdhim_rs_t struct
    md->p->mdhim_rs = (mdhim_rs_t*)malloc(sizeof(struct mdhim_rs_t));
    if (!md->p->mdhim_rs) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - "
             "Error while allocating memory for range server",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        return MDHIM_ERROR;
    }

    //Initialize variables for printing out timings
    md->p->mdhim_rs->put_time = 0;
    md->p->mdhim_rs->get_time = 0;
    md->p->mdhim_rs->num_put = 0;
    md->p->mdhim_rs->num_get = 0;
    //Initialize work queue
    md->p->mdhim_rs->work_queue = (work_queue_t*)malloc(sizeof(work_queue_t));
    md->p->mdhim_rs->work_queue->head = NULL;
    md->p->mdhim_rs->work_queue->tail = NULL;

    //Initialize the outstanding request list
    md->p->mdhim_rs->out_req_list = NULL;

    //Initialize work queue mutex
    md->p->mdhim_rs->work_queue_mutex = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
    if (!md->p->mdhim_rs->work_queue_mutex) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - "
             "Error while allocating memory for range server",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        return MDHIM_ERROR;
    }
    if ((ret = pthread_mutex_init(md->p->mdhim_rs->work_queue_mutex, NULL)) != 0) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - "
             "Error while initializing work queue mutex", ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        return MDHIM_ERROR;
    }

    //Initialize out req mutex
    md->p->mdhim_rs->out_req_mutex = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
    if (!md->p->mdhim_rs->out_req_mutex) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - "
             "Error while allocating memory for range server",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        return MDHIM_ERROR;
    }
    if ((ret = pthread_mutex_init(md->p->mdhim_rs->out_req_mutex, NULL)) != 0) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - "
             "Error while initializing out req mutex", ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        return MDHIM_ERROR;
    }

    //Initialize the condition variables
    md->p->mdhim_rs->work_ready_cv = (pthread_cond_t*)malloc(sizeof(pthread_cond_t));
    if (!md->p->mdhim_rs->work_ready_cv) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - "
             "Error while allocating memory for range server",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        return MDHIM_ERROR;
    }
    if ((ret = pthread_cond_init(md->p->mdhim_rs->work_ready_cv, NULL)) != 0) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - "
             "Error while initializing condition variable",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        return MDHIM_ERROR;
    }

    //Initialize worker threads
    md->p->mdhim_rs->workers = (pthread_t**)malloc(sizeof(pthread_t *) * md->p->db_opts->num_wthreads);
    for (i = 0; i < md->p->db_opts->num_wthreads; i++) {
        md->p->mdhim_rs->workers[i] = (pthread_t*)malloc(sizeof(pthread_t));
        if ((ret = pthread_create(md->p->mdhim_rs->workers[i], NULL,
                      worker_thread, (void *) md)) != 0) {
            mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - "
                 "Error while initializing worker thread",
                 ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
            return MDHIM_ERROR;
        }
    }

    //Initialize listener threads
    if ((ret = pthread_create(&md->p->mdhim_rs->listener, NULL,
                  listener_thread, (void *) md)) != 0) {
      mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - "
           "Error while initializing listener thread",
           ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
      return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}
