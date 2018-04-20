/*
 * MDHIM TNG
 *
 * Client specific implementation
 */

#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "range_server.h"
#include "partitioner.h"
#include "mdhim_options.h"
#include "mdhim_options_private.h"
#include "mdhim_private.h"

static void add_timing(struct timeval start, struct timeval end, int num,
                       mdhim_t *md, TransportMessageType mtype) {
    long double elapsed;

    elapsed = (long double) (end.tv_sec - start.tv_sec) +
        ((long double) (end.tv_usec - start.tv_usec)/1000000.0);
    if (mtype == TransportMessageType::PUT || mtype == TransportMessageType::BPUT) {
        md->p->mdhim_rs->put_time += elapsed;
        md->p->mdhim_rs->num_put += num;
    } else if (mtype == TransportMessageType::BGET) {
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
static int send_locally_or_remote(mdhim_t *md, work_item_t *item, TransportMessage *message) {
    int ret = MDHIM_SUCCESS;
    if (md->rank != message->dst) {
        //Send the message remotely
        ret = md->p->send_client_response(item, message, md->p->shutdown);
    } else {
        //Send the message locally
        pthread_mutex_lock(&md->p->receive_msg_mutex);
        md->p->receive_msg = message;

        pthread_cond_signal(&md->p->receive_msg_ready_cv);
        pthread_mutex_unlock(&md->p->receive_msg_mutex);
    }

    return ret;
}

static index_t *find_index(mdhim_t *md, TransportMessage *msg) {
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
static index_t * find_index_by_name(mdhim_t *md, TransportMessage *msg) {
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
int range_server_add_work(mdhim_t *md, work_item_t *item) {
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

static work_item_t *get_work(mdhim_t *md) {
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
int range_server_stop(mdhim_t *md) {
    if (!md || !md->p) {
        return MDHIM_ERROR;
    }

    int ret;

    //Signal to the listener thread that it needs to shutdown
    md->p->shutdown = 1;

    /* Wait for the threads to finish */
    pthread_mutex_lock(md->p->mdhim_rs->work_queue_mutex);
    pthread_cond_broadcast(md->p->mdhim_rs->work_ready_cv);
    pthread_mutex_unlock(md->p->mdhim_rs->work_queue_mutex);

    /* Wait for the threads to finish */
    for (int i = 0; i < md->p->db_opts->num_wthreads; i++) {
        pthread_join(*md->p->mdhim_rs->workers[i], NULL);
        delete md->p->mdhim_rs->workers[i];
    }
    delete [] md->p->mdhim_rs->workers;

    //Destroy the condition variables
    if ((ret = pthread_cond_destroy(md->p->mdhim_rs->work_ready_cv)) != 0) {
      mlog(MDHIM_SERVER_DBG, "Rank %d - Error destroying work cond variable",
           md->rank);
    }
    delete md->p->mdhim_rs->work_ready_cv;

    //Destroy the work queue mutex
    if ((ret = pthread_mutex_destroy(md->p->mdhim_rs->work_queue_mutex)) != 0) {
      mlog(MDHIM_SERVER_DBG, "Rank %d - Error destroying work queue mutex",
           md->rank);
    }
    delete md->p->mdhim_rs->work_queue_mutex;

    //Clean outstanding sends
    range_server_clean_oreqs(md);
    //Destroy the out req mutex
    if ((ret = pthread_mutex_destroy(md->p->mdhim_rs->out_req_mutex)) != 0) {
      mlog(MDHIM_SERVER_DBG, "Rank %d - Error destroying work queue mutex",
           md->rank);
    }
    delete md->p->mdhim_rs->out_req_mutex;

    //Free the work queue
    work_item_t *head = md->p->mdhim_rs->work_queue->head;
    while (head) {
      work_item_t *temp_item = head->next;
      delete head;
      head = temp_item;
    }
    delete md->p->mdhim_rs->work_queue;

    mlog(MDHIM_SERVER_INFO, "Rank %d - Inserted:  %ld records in %Lf seconds",
         md->rank, md->p->mdhim_rs->num_put, md->p->mdhim_rs->put_time);
    mlog(MDHIM_SERVER_INFO, "Rank %d - Retrieved: %ld records in %Lf seconds",
         md->rank, md->p->mdhim_rs->num_get, md->p->mdhim_rs->get_time);

    //Free the range server data
    delete md->p->mdhim_rs;
    md->p->mdhim_rs = nullptr;

    return MDHIM_SUCCESS;
}

/**
 * range_server_put
 * Handles the put message and puts data in the database
 *
 * @param md        pointer to the main MDHIM struct
 * @param im        pointer to the put message to handle
 * @param item      the source work load, which includes the message and originator of the message
 * @return          MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int range_server_put(mdhim_t *md, work_item_t *item) {
    int ret;
    int error = 0;
    int exists = 0;
    void *new_value;
    int32_t new_value_len;
    void *old_value;
    int32_t old_value_len;
    struct timeval start, end;
    int inserted = 0;

    TransportPutMessage *im = dynamic_cast<TransportPutMessage *>(item->message);
    item->message = nullptr;

    void **value = new void *();
    *value = nullptr;

    int32_t *value_len = new int32_t();
    *value_len = 0;

    //Get the index referenced the message
    index_t *index = find_index(md, (TransportMessage *) im);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error retrieving index for id: %d",
             md->rank, im->index);
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
    if (exists && md->p->db_opts->value_append == MDHIM_DB_APPEND) {
        old_value = *value;
        old_value_len = *value_len;
        new_value_len = old_value_len + im->value_len;
        new_value = ::operator new(new_value_len);
        memcpy(new_value, old_value, old_value_len);
        memcpy((char*)new_value + old_value_len, im->value, im->value_len);
    } else {
        new_value = im->value;
        new_value_len = im->value_len;
    }

    if (*value && *value_len) {
        ::operator delete(*value);
    }
    delete value;
    delete value_len;

    //Put the record in the database
    if ((ret =
         index->mdhim_store->put(index->mdhim_store->db_handle,
                     im->key, im->key_len, new_value,
                     new_value_len)) != MDHIM_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error putting record",
             md->rank);
        error = ret;
    } else {
        inserted = 1;
    }

    if (!exists && error == MDHIM_SUCCESS) {
        update_stat(md, index, im->key, im->key_len);
    }

    gettimeofday(&end, NULL);
    add_timing(start, end, inserted, md, TransportMessageType::PUT);

done:
    //Create the response message
    TransportRecvMessage *rm = new TransportRecvMessage();
    //Set the type
    rm->mtype = TransportMessageType::RECV;
    //Set the operation return code as the error
    rm->error = error;
    rm->src = md->rank;
    rm->dst = im->src;

    //Free memory
    if (exists && md->p->db_opts->value_append == MDHIM_DB_APPEND) {
        ::operator delete(new_value);
    }
    if (im->src != md->rank) {
        ::operator delete(im->key);
        im->key = nullptr;

        ::operator delete(im->value);
        im->value = nullptr;
    }

    delete im;

    //Send response
    return send_locally_or_remote(md, item, rm);
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
static int range_server_bput(mdhim_t *md, work_item_t *item) {
    int ret;
    int error = MDHIM_SUCCESS;
    int32_t value_len;
    void *new_value;
    int32_t new_value_len;
    void *old_value;
    int32_t old_value_len;
    struct timeval start, end;
    int num_put = 0;

    gettimeofday(&start, NULL);

    TransportBPutMessage *bim = dynamic_cast<TransportBPutMessage *>(item->message);
    item->message = nullptr;

    int *exists = new int[bim->num_keys]();
    void **new_values = new void *[bim->num_keys]();
    int32_t *new_value_lens = new int32_t[bim->num_keys]();
    void **value = new void *();

    //Get the index referenced the message
    index_t *index = find_index(md, (TransportMessage *)bim);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error retrieving index for id: %d",
             md->rank, bim->index);
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
        if (exists[i] && md->p->db_opts->value_append == MDHIM_DB_APPEND) {
            old_value = *value;
            old_value_len = value_len;
            new_value_len = old_value_len + bim->value_lens[i];
            new_value = ::operator new(new_value_len);
            memcpy(new_value, old_value, old_value_len);
            memcpy((char*)new_value + old_value_len, bim->values[i], bim->value_lens[i]);
            if (exists[i] && bim->src != md->rank) {
                ::operator delete(bim->values[i]);
                bim->values[i] = nullptr;
            }

            new_values[i] = new_value;
            new_value_lens[i] = new_value_len;
        } else {
            new_values[i] = bim->values[i];
            new_value_lens[i] = bim->value_lens[i];
        }

        if (*value) {
            // value comes from leveldb
            free(*value);
            *value = nullptr;
        }
    }

    //Put the record in the database
    if ((ret =
         index->mdhim_store->batch_put(index->mdhim_store->db_handle,
                                       bim->keys, bim->key_lens, new_values,
                                       new_value_lens, bim->num_keys)) != MDHIM_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error batch putting records",
             md->rank);
        error = ret;
    } else {
        num_put = bim->num_keys;
    }

    for (int i = 0; i < bim->num_keys && i < MAX_BULK_OPS; i++) {
        //Update the stats if this key didn't exist before
        if (!exists[i] && error == MDHIM_SUCCESS) {
            update_stat(md, index, bim->keys[i], bim->key_lens[i]);
        }

        if (exists[i] && md->p->db_opts->value_append == MDHIM_DB_APPEND) {
            //Release the value created for appending the new and old value
            ::operator delete(new_values[i]);
        }

        //Release the bput keys/value if the message isn't coming from myself
        if (bim->src != md->rank) {
            ::operator delete(bim->keys[i]);
            bim->keys[i] = nullptr;
            ::operator delete(bim->values[i]);
            bim->values[i] = nullptr;
        }
    }

    delete [] exists;
    delete [] new_values;
    delete [] new_value_lens;
    delete value;
    gettimeofday(&end, NULL);
    add_timing(start, end, num_put, md, TransportMessageType::BPUT);

 done:

    //Create the response message
    TransportRecvMessage *brm = new TransportRecvMessage();

    //Set the type
    brm->mtype = TransportMessageType::RECV;
    //Set the operation return code as the error
    brm->error = error;
    brm->src = md->rank;
    brm->dst = bim->src;

    //Release the internals of the bput message
    delete bim;

    //Send response
    return send_locally_or_remote(md, item, brm);
}

/**
 * range_server_del
 * Handles the delete message and deletes the data from the database
 *
 * @param md       Pointer to the main MDHIM struct
 * @param dm       pointer to the delete message to handle
 * @param source   source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int range_server_del(mdhim_t *md, work_item_t *item) {
    int ret = MDHIM_ERROR;

    TransportDeleteMessage *dm = dynamic_cast<TransportDeleteMessage *>(item->message);
    item->message = nullptr;

    //Get the index referenced the message
    index_t *index = find_index(md, (TransportMessage *)dm);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error retrieving index for id: %d",
             md->rank, dm->index);
        ret = MDHIM_ERROR;
        goto done;
    }

    //Delete the record from the database
    if ((ret =
         index->mdhim_store->del(index->mdhim_store->db_handle,
                                 dm->key, dm->key_len)) != MDHIM_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error deleting record",
             md->rank);
    }

 done:
    //Create the response message
    TransportRecvMessage *rm = new TransportRecvMessage();
    //Set the type
    rm->mtype = TransportMessageType::RECV;
    //Set the operation return code as the error
    rm->error = ret;
    rm->src = md->rank;
    rm->dst = dm->src;

    delete dm;

    //Send response
    return send_locally_or_remote(md, item, rm);
}

/**
 * range_server_bdel
 * Handles the bulk delete message and deletes the data from the database
 *
 * @param md        Pointer to the main MDHIM struct
 * @param bdm       pointer to the bulk delete message to handle
 * @param source    source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bdel(mdhim_t *md, work_item_t *item) {
    int ret;
    int error = 0;

    TransportBDeleteMessage *bdm = dynamic_cast<TransportBDeleteMessage *>(item->message);
    item->message = nullptr;

    //Get the index referenced the message
    index_t *index = find_index(md, (TransportMessage *)bdm);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error retrieving index for id: %d",
             md->rank, bdm->index);
        error = MDHIM_ERROR;
        goto done;
    }

    //Iterate through the arrays and delete each record
    for (int i = 0; i < bdm->num_keys && i < MAX_BULK_OPS; i++) {
        //Put the record in the database
        if ((ret =
             index->mdhim_store->del(index->mdhim_store->db_handle,
                                     bdm->keys[i], bdm->key_lens[i]))
            != MDHIM_SUCCESS) {
            mlog(MDHIM_SERVER_CRIT, "Rank %d - Error deleting record",
                 md->rank);
            error = ret;
        }
    }

done:
    //Create the response message
    TransportRecvMessage *brm = new TransportRecvMessage();
    //Set the type
    brm->mtype = TransportMessageType::RECV;
    //Set the operation return code as the error
    brm->error = error;
    brm->src = md->rank;
    brm->dst = bdm->src;

    delete bdm;

    //Send response
    return send_locally_or_remote(md, item, brm);
}

/**
 * range_server_commit
 * Handles the commit message and commits outstanding writes to the database
 *
 * @param md        pointer to the main MDHIM struct
 * @param im        pointer to the commit message to handle
 * @param source    source of the message
 * @return          MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int range_server_commit(mdhim_t *md, work_item_t *item) {
    int ret;
    index_t *index;

    TransportMessage *im = item->message;
    item->message = nullptr;

    //Get the index referenced the message
    index = find_index(md, im);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error retrieving index for id: %d",
             md->rank, im->index);
        ret = MDHIM_ERROR;
        goto done;
    }

        //Put the record in the database
    if ((ret =
         index->mdhim_store->commit(index->mdhim_store->db_handle))
        != MDHIM_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error committing database",
             md->rank);
    }

 done:
    //Create the response message
    TransportRecvMessage *rm = new TransportRecvMessage();
    //Set the type
    rm->mtype = TransportMessageType::RECV;
    //Set the operation return code as the error
    rm->error = ret;
    rm->src = md->rank;
    rm->dst = im->src;

    delete im;

    //Send response
    return send_locally_or_remote(md, item, rm);
}

/**
 * range_server_get
 * Handles the get message, retrieves the data from the database, and sends the results back
 *
 * @param md        Pointer to the main MDHIM struct
 * @param gm        pointer to the get message to handle
 * @param source    source of the message
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int range_server_get(mdhim_t *md, work_item_t *item) {
    int ret;
    void *value = nullptr;
    int32_t value_len = 0;
    int error = 0;
    struct timeval start, end;
    int num_retrieved = 0;

    gettimeofday(&start, NULL);

    TransportGetMessage *gm = dynamic_cast<TransportGetMessage *>(item->message);
    item->message = nullptr;

    //Get the index referenced the message
    index_t *index = find_index(md, (TransportMessage *) gm);
    if (!index) {
        error = MDHIM_ERROR;
        goto done;
    }

    switch(gm->op) {
        // Gets the value for the given key
        case TransportGetMessageOp::GET_EQ:
            if ((ret =
                 index->mdhim_store->get(index->mdhim_store->db_handle,
                                         gm->key, gm->key_len, &value,
                                         &value_len)) != MDHIM_SUCCESS) {
                error = ret;
                value_len = 0;
                value = nullptr;
            }

            break;
        /* Gets the next key and value that is in order after the passed in key */
        case TransportGetMessageOp::GET_NEXT:
            if ((ret =
                 index->mdhim_store->get_next(index->mdhim_store->db_handle,
                                              &gm->key, &gm->key_len, &value,
                                              &value_len)) != MDHIM_SUCCESS) {
                error = ret;
                value_len = 0;
                value = nullptr;
            }

            break;
        /* Gets the previous key and value that is in order before the passed in key
           or the last key if no key was passed in */
        case TransportGetMessageOp::GET_PREV:
            if ((ret =
                 index->mdhim_store->get_prev(index->mdhim_store->db_handle,
                                              &gm->key, &gm->key_len, &value,
                                              &value_len)) != MDHIM_SUCCESS) {
                error = ret;
                value_len = 0;
                value = nullptr;
            }

            break;
        /* Gets the first key/value */
        case TransportGetMessageOp::GET_FIRST:
            if ((ret =
                 index->mdhim_store->get_next(index->mdhim_store->db_handle,
                                              &gm->key, 0, &value,
                                              &value_len)) != MDHIM_SUCCESS) {
                error = ret;
                value_len = 0;
                value = nullptr;
            }

            break;
        /* Gets the last key/value */
        case TransportGetMessageOp::GET_LAST:
            if ((ret =
                 index->mdhim_store->get_prev(index->mdhim_store->db_handle,
                                              &gm->key, 0, &value,
                                              &value_len)) != MDHIM_SUCCESS) {
                error = ret;
                value_len = 0;
                value = nullptr;
            }

            break;
        default:
            break;
    }

    gettimeofday(&end, NULL);
    add_timing(start, end, num_retrieved, md, TransportMessageType::BGET);

done:
    //Create the response message
    TransportGetRecvMessage *grm = new TransportGetRecvMessage();
    //Set the type
    grm->mtype = TransportMessageType::RECV_GET;
    //Set the operation return code as the error
    grm->error = error;
    grm->src = md->rank;
    grm->dst = gm->src;
    //Set the key and value

    if (gm->src == md->rank) {
        //If this message is coming from myself, copy the keys
        grm->key = ::operator new(gm->key_len);
        memcpy(grm->key, gm->key, gm->key_len);
        ::operator delete(gm->key);
    } else {
        grm->key = gm->key;
    }

    gm->key = nullptr;
    grm->key_len = gm->key_len;

    // clone the value
    grm->value = ::operator new(value_len);
    memcpy(grm->value, value, value_len);
    free(value); // cleanup leveldb

    grm->value_len = value_len;

    grm->index = index->id;
    grm->index_type = index->type;

    //Release the bget message
    delete gm;

    //Send response
    return send_locally_or_remote(md, item, grm);
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
static int range_server_bget(mdhim_t *md, work_item_t *item) {
    int ret;
    int error = 0;
    struct timeval start, end;
    int num_retrieved = 0;

    gettimeofday(&start, NULL);

    TransportBGetMessage *bgm = dynamic_cast<TransportBGetMessage *>(item->message);
    item->message = nullptr;

    void **values = new void *[bgm->num_keys]();
    int32_t *value_lens = new int32_t[bgm->num_keys]();

    //Get the index referenced the message
    index_t *index = find_index(md, (TransportMessage *) bgm);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error retrieving index for id: %d",
             md->rank, bgm->index);
        error = MDHIM_ERROR;
        goto done;
    }

    //Iterate through the arrays and get each record
    for (int i = 0; i < bgm->num_keys && i < MAX_BULK_OPS; i++) {
        switch(bgm->op) {
            // Gets the value for the given key
            case TransportGetMessageOp::GET_EQ:
                if ((ret =
                     index->mdhim_store->get(index->mdhim_store->db_handle,
                                             bgm->keys[i], bgm->key_lens[i], &values[i],
                                             &value_lens[i])) != MDHIM_SUCCESS) {
                    error = ret;
                    value_lens[i] = 0;
                    values[i] = nullptr;
                    continue;
                }
                break;
            /* Gets the next key and value that is in order after the passed in key */
            case TransportGetMessageOp::GET_NEXT:
                if ((ret =
                     index->mdhim_store->get_next(index->mdhim_store->db_handle,
                                                  &bgm->keys[i], &bgm->key_lens[i], &values[i],
                                                  &value_lens[i])) != MDHIM_SUCCESS) {
                    mlog(MDHIM_SERVER_DBG, "Rank %d - Error getting record", md->rank);
                    error = ret;
                    value_lens[i] = 0;
                    values[i] = nullptr;
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
                    mlog(MDHIM_SERVER_DBG, "Rank %d - Error getting record", md->rank);
                    error = ret;
                    value_lens[i] = 0;
                    values[i] = nullptr;
                    continue;
                }

                break;
             /* Gets the first key/value */
            case TransportGetMessageOp::GET_FIRST:
                if ((ret =
                     index->mdhim_store->get_next(index->mdhim_store->db_handle,
                                                  &bgm->keys[i], 0, &values[i],
                                                  &value_lens[i])) != MDHIM_SUCCESS) {
                    mlog(MDHIM_SERVER_DBG, "Rank %d - Error getting record", md->rank);
                    error = ret;
                    value_lens[i] = 0;
                    values[i] = nullptr;
                    continue;
                }

                break;
            /* Gets the last key/value */
            case TransportGetMessageOp::GET_LAST:
                if ((ret =
                     index->mdhim_store->get_prev(index->mdhim_store->db_handle,
                                                  &bgm->keys[i], 0, &values[i],
                                                  &value_lens[i])) != MDHIM_SUCCESS) {
                    mlog(MDHIM_SERVER_DBG, "Rank %d - Error getting record", md->rank);
                    error = ret;
                    value_lens[i] = 0;
                    values[i] = nullptr;
                    continue;
                }

                break;
            default:
                mlog(MDHIM_SERVER_DBG, "Rank %d - Invalid operation: %d given in range_server_get",
                     md->rank, (int) bgm->op);
                continue;
        }

        num_retrieved++;
    }

    gettimeofday(&end, NULL);
    add_timing(start, end, num_retrieved, md, TransportMessageType::BGET);

done:

    //Create the response message
    TransportBGetRecvMessage *bgrm = new TransportBGetRecvMessage();
    //Set the type
    bgrm->mtype = TransportMessageType::RECV_BGET;
    //Set the operation return code as the error
    bgrm->error = error;
    bgrm->src = md->rank;
    bgrm->dst = bgm->src;
    bgrm->num_keys = bgm->num_keys;
    bgrm->index = index?index->id:-1;
    bgrm->index_type = index?index->type:BAD_INDEX;

    // copy the values
    bgrm->values = new void *[bgm->num_keys]();
    bgrm->value_lens = new int[bgm->num_keys]();
    for (int i = 0; i < bgm->num_keys; i++) {
        if (values[i]) {
            bgrm->values[i] = ::operator new(value_lens[i]);
            memcpy(bgrm->values[i], values[i], value_lens[i]);
            bgrm->value_lens[i] = value_lens[i];
        }

        // cleanup leveldb
        free(values[i]);
    }

    delete [] values;
    delete [] value_lens;

    //Set the keys into bgrm
    if (bgm->src == md->rank) {
        //If this message is coming from myself, copy the keys
        bgrm->key_lens = new int[bgm->num_keys]();
        bgrm->keys = new void *[bgm->num_keys]();
        for (int i = 0; i < bgm->num_keys; i++) {
            bgrm->key_lens[i] = bgm->key_lens[i];
            bgrm->keys[i] = ::operator new(bgrm->key_lens[i]);
            memcpy(bgrm->keys[i], bgm->keys[i], bgm->key_lens[i]);
        }
    } else {
        bgrm->keys = bgm->keys;
        bgrm->key_lens = bgm->key_lens;
        bgm->keys = nullptr;
        bgm->key_lens = nullptr;
        bgm->num_keys = 0;
    }

    //Release the bget message
    delete bgm;

    //Send response
    return send_locally_or_remote(md, item, bgrm);
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
static int range_server_bget_op(mdhim_t *md, work_item_t *item, TransportGetMessageOp op) {
    int error = 0;
    int ret;
    struct timeval start, end;

    TransportBGetMessage *bgm = dynamic_cast<TransportBGetMessage *>(item->message);
    item->message = nullptr;

    //Initialize pointers and lengths
    const int count = bgm->num_keys * bgm->num_recs;
    void **values = new void *[count]();
    int32_t *value_lens = new int32_t[count]();

    void **keys = new void *[count];
    int32_t *key_lens = new int32_t[count];

    //Used for passing the key and key len to the db
    void **get_key = new void *();
    int32_t *get_key_len = new int32_t();

    //Used for passing the value and value len to the db
    void **get_value = new void *();
    int32_t *get_value_len = new int32_t();

    int num_records = 0;

    //Get the index referenced the message
    index_t *index = find_index(md, (TransportMessage *) bgm);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error retrieving index for id: %d",
             md->rank, bgm->index);
        error = MDHIM_ERROR;
        goto respond;
    }

    if (bgm->num_keys * bgm->num_recs > MAX_BULK_OPS) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Too many bulk operations requested",
             md->rank);
        error = MDHIM_ERROR;
        goto respond;
    }

    mlog(MDHIM_SERVER_CRIT, "Rank %d - Num keys is: %d and num recs is: %d",
         md->rank, bgm->num_keys, bgm->num_recs);
    gettimeofday(&start, NULL);
    //Iterate through the arrays and get each record
    for (int i = 0; i < bgm->num_keys; i++) {
        for (int j = 0; j < bgm->num_recs; j++) {
            keys[num_records] = NULL;
            key_lens[num_records] = 0;

            //If we were passed in a key, copy it
            if (!j && bgm->key_lens[i] && bgm->keys[i]) {
                *get_key = ::operator new(bgm->key_lens[i]);
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
                    mlog(MDHIM_SERVER_DBG, "Rank %d - Couldn't get next record",
                         md->rank);
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
                    mlog(MDHIM_SERVER_DBG, "Rank %d - Couldn't get prev record",
                         md->rank);
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
                mlog(MDHIM_SERVER_CRIT, "Rank %d - Invalid operation for bulk get op",
                     md->rank);
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
    add_timing(start, end, num_records, md, TransportMessageType::BGET);

    //Create the response message
    TransportBGetRecvMessage *bgrm = new TransportBGetRecvMessage();
    //Set the type
    bgrm->mtype = TransportMessageType::RECV_BGET;
    //Set the operation return code as the error
    bgrm->error = error;
    bgrm->src = md->rank;
    bgrm->dst = bgm->src;
    //Set the keys and values
    bgrm->keys = keys;
    bgrm->key_lens = key_lens;
    bgrm->values = values;
    bgrm->value_lens = value_lens;
    bgrm->num_keys = num_records;
    bgrm->index = index->id;
    bgrm->index_type = index->type;

    //Free stuff
    if (bgm->src == md->rank) {
        /* If this message is not coming from myself,
           free the keys and values from the get message */
        // mdhim_partial_release_msg(bgm);
        delete bgm;
    }

    delete get_key;
    delete get_key_len;
    delete get_value;
    delete get_value_len;

    // Send response
    return send_locally_or_remote(md, item, bgrm);
}

/*
 * worker_thread
 * Function for the thread that processes work in the work queue
 */
static void *worker_thread(void *data) {
    //Mlog statements could cause a deadlock on range_server_stop due to canceling of threads
    mdhim_t *md = (mdhim_t *) data;
    work_item_t *item = nullptr;

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
        if (!(item = get_work(md))) {
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

        //Call the appropriate function depending on the message type
        while (item) {
            switch(item->message->mtype) {
            case TransportMessageType::PUT:
                range_server_put(md, item);
                break;
            case TransportMessageType::GET:
                range_server_get(md, item);
                break;
            case TransportMessageType::BPUT:
                range_server_bput(md, item);
                break;
            case TransportMessageType::BGET:
                {
                    TransportBGetMessage *bgm = dynamic_cast<TransportBGetMessage *>(item->message);
                    //The client is sending one key, but requesting the retrieval of more than one
                    if (bgm->num_recs > 1 && bgm->num_keys == 1) {
                        range_server_bget_op(md, item, bgm->op);
                    }
                    else {
                        range_server_bget(md, item);
                    }
                }
                break;
            case TransportMessageType::DELETE:
                range_server_del(md, item);
                break;
            case TransportMessageType::BDELETE:
                range_server_bdel(md, item);
                break;
            case TransportMessageType::COMMIT:
                range_server_commit(md, item);
                break;
            default:
                break;
            }

            work_item_t *item_tmp = item;
            item = item->next;
            delete item_tmp;
        }

        //Clean outstanding sends
        range_server_clean_oreqs(md);
    }

    return NULL;
}

int range_server_add_oreq(mdhim_t *md, MPI_Request *req, void *msg) {
    out_req_t *oreq;
    out_req_t *item;

    pthread_mutex_lock(md->p->mdhim_rs->out_req_mutex);
    item = md->p->mdhim_rs->out_req_list;
    oreq = new out_req_t();
    oreq->next = NULL;
    oreq->prev = NULL;
    oreq->message = msg;
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

int range_server_clean_oreqs(mdhim_t *md) {
    pthread_mutex_lock(md->p->mdhim_rs->out_req_mutex);
    out_req_t *item = md->p->mdhim_rs->out_req_list;
    while (item) {
        if (!item->req) {
            item = item->next;
            continue;
        }

        pthread_mutex_lock(&md->lock);
        int flag = 0;
        MPI_Status status;
        MPI_Test(item->req, &flag, &status);
        pthread_mutex_unlock(&md->lock);

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

        out_req_t *t = item->next;
        ::operator delete(item->req);
        if (item->message) {
            ::operator delete(item->message);
        }

        delete item;
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
int range_server_init(mdhim_t *md) {
    int ret;

    //Allocate memory for the mdhim_rs_t struct
    md->p->mdhim_rs = new mdhim_rs_t();
    if (!md->p->mdhim_rs) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while allocating memory for range server",
             md->rank);
        return MDHIM_ERROR;
    }

    //Initialize variables for printing out timings
    md->p->mdhim_rs->put_time = 0;
    md->p->mdhim_rs->get_time = 0;
    md->p->mdhim_rs->num_put = 0;
    md->p->mdhim_rs->num_get = 0;
    //Initialize work queue
    md->p->mdhim_rs->work_queue = new work_queue_t();
    md->p->mdhim_rs->work_queue->head = NULL;
    md->p->mdhim_rs->work_queue->tail = NULL;

    //Initialize the outstanding request list
    md->p->mdhim_rs->out_req_list = NULL;

    //Initialize work queue mutex
    md->p->mdhim_rs->work_queue_mutex = new pthread_mutex_t();
    if (!md->p->mdhim_rs->work_queue_mutex) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while allocating memory for range server",
             md->rank);
        return MDHIM_ERROR;
    }
    if ((ret = pthread_mutex_init(md->p->mdhim_rs->work_queue_mutex, NULL)) != 0) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while initializing work queue mutex", md->rank);
        return MDHIM_ERROR;
    }

    //Initialize out req mutex
    md->p->mdhim_rs->out_req_mutex = new pthread_mutex_t();
    if (!md->p->mdhim_rs->out_req_mutex) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while allocating memory for range server",
             md->rank);
        return MDHIM_ERROR;
    }
    if ((ret = pthread_mutex_init(md->p->mdhim_rs->out_req_mutex, NULL)) != 0) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while initializing out req mutex", md->rank);
        return MDHIM_ERROR;
    }

    //Initialize the condition variables
    md->p->mdhim_rs->work_ready_cv = new pthread_cond_t();
    if (!md->p->mdhim_rs->work_ready_cv) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while allocating memory for range server",
             md->rank);
        return MDHIM_ERROR;
    }
    if ((ret = pthread_cond_init(md->p->mdhim_rs->work_ready_cv, NULL)) != 0) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while initializing condition variable",
             md->rank);
        return MDHIM_ERROR;
    }

    //Initialize worker threads
    md->p->mdhim_rs->workers = new pthread_t *[md->p->db_opts->num_wthreads]();
    for (int i = 0; i < md->p->db_opts->num_wthreads; i++) {
        md->p->mdhim_rs->workers[i] = new pthread_t();
        if ((ret = pthread_create(md->p->mdhim_rs->workers[i], NULL,
                      worker_thread, (void *) md)) != 0) {
            mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
                 "Error while initializing worker thread",
                 md->rank);
            return MDHIM_ERROR;
        }
    }

    return MDHIM_SUCCESS;
}
