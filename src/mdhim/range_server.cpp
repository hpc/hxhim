/*
 * MDHIM TNG
 *
 * Client specific implementation
 */

#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "clone.hpp"
#include "range_server.h"
#include "partitioner.h"
#include "mdhim_options.h"
#include "mdhim_options_private.h"
#include "mdhim_private.h"

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
    pthread_mutex_lock(&md->p->mdhim_rs->work_queue_mutex);
    item->next = nullptr;
    item->prev = nullptr;

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
    pthread_cond_signal(&md->p->mdhim_rs->work_ready_cv);
    pthread_mutex_unlock(&md->p->mdhim_rs->work_queue_mutex);

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
        return nullptr;
    }

    //Set the list head and tail to nullptr
    md->p->mdhim_rs->work_queue->head = nullptr;
    md->p->mdhim_rs->work_queue->tail = nullptr;

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

    //Signal to the listener thread that it needs to shutdown
    md->p->shutdown = 1;

    /* Wait for the threads to finish */
    pthread_mutex_lock(&md->p->mdhim_rs->work_queue_mutex);
    pthread_cond_broadcast(&md->p->mdhim_rs->work_ready_cv);
    pthread_mutex_unlock(&md->p->mdhim_rs->work_queue_mutex);

    /* Wait for the threads to finish */
    for (int i = 0; i < md->p->db_opts->num_wthreads; i++) {
        pthread_join(*md->p->mdhim_rs->workers[i], nullptr);
        delete md->p->mdhim_rs->workers[i];
    }
    delete [] md->p->mdhim_rs->workers;

    //Clean outstanding sends
    range_server_clean_oreqs(md);

    //Free the work queue
    work_item_t *head = md->p->mdhim_rs->work_queue->head;
    while (head) {
        work_item_t *temp_item = head->next;
        delete head;
        head = temp_item;
    }
    delete md->p->mdhim_rs->work_queue;

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
    bool exists = false;
    void *new_value;
    std::size_t new_value_len;
    void *old_value;
    std::size_t old_value_len;
    int inserted = 0;

    TransportPutMessage *im = dynamic_cast<TransportPutMessage *>(item->message);

    void *value = nullptr;
    std::size_t value_len = 0;

    //Get the index referenced the message
    index_t *index = find_index(md, (TransportMessage *) im);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error retrieving index for id: %d",
             md->rank, im->index);
        error = MDHIM_ERROR;
        goto done;
    }

    // //Check for the key's existence
    // index->mdhim_stores[im->rs_idx]->get(index->mdhim_stores[im->rs_idx]->db_handle,
    //                                      im->key, im->key_len, &value, &value_len);
    // //The key already exists
    // exists = (value && value_len);

    //If the option to append was specified and there is old data, concat the old and new
    if (exists && md->p->db_opts->value_append == MDHIM_DB_APPEND) {
        old_value = value;
        old_value_len = value_len;
        new_value_len = old_value_len + im->value_len;
        new_value = ::operator new(new_value_len);
        memcpy(new_value, old_value, old_value_len);
        memcpy((char*)new_value + old_value_len, im->value, im->value_len);
        free(value);
    } else {
        new_value = im->value;
        new_value_len = im->value_len;
    }

    //Put the record in the database
    if ((ret =
         index->mdhim_stores[im->rs_idx]->put(index->mdhim_stores[im->rs_idx]->db_handle,
                                              im->key, im->key_len, new_value,
                                              new_value_len)) != MDHIM_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error putting record",
             md->rank);
        error = ret;
    } else {
        inserted = 1;
    }

    if (!exists && error == MDHIM_SUCCESS) {
        update_stat(md, index, im->rs_idx, im->key, im->key_len);
    }

  done:
    //Create the response message
    TransportRecvMessage *rm = new TransportRecvMessage();
    rm->error = error;
    rm->src = im->dst;
    rm->dst = im->src;
    rm->rs_idx = im->rs_idx;

    //Free memory
    if (exists && md->p->db_opts->value_append == MDHIM_DB_APPEND) {
        ::operator delete(new_value);
    }

    // only delete the key and value if they were unpacked
    if (im->src != md->rank) {
        ::operator delete(im->key);
        ::operator delete(im->value);
    }

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
    int num_put = 0;

    TransportBPutMessage *bim = dynamic_cast<TransportBPutMessage *>(item->message);

    //Get the index referenced the message
    index_t *index = find_index(md, (TransportMessage *)bim);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error retrieving index for id: %d",
             md->rank, bim->index);
        error = MDHIM_ERROR;
        goto done;
    }

    //Iterate through the arrays and insert each record
    for (std::size_t i = 0; i < bim->num_keys && i < MAX_BULK_OPS; i++) {
        void *old_value = nullptr;
        std::size_t old_value_len = 0;

        //Check for the key's existence
        index->mdhim_stores[bim->rs_idx[i]]->get(index->mdhim_stores[bim->rs_idx[i]]->db_handle,
                                                 bim->keys[i], bim->key_lens[i],
                                                 &old_value, &old_value_len);
        //The key already exists
        const bool exists = (old_value && old_value_len);

        //If the option to append was specified and there is old data, concat the old and new
        void *new_value = nullptr;
        std::size_t new_value_len;
        if (exists && md->p->db_opts->value_append == MDHIM_DB_APPEND) {
            new_value_len = old_value_len + bim->value_lens[i];
            new_value = ::operator new(new_value_len);
            memcpy(new_value, old_value, old_value_len);
            memcpy((char*)new_value + old_value_len, bim->values[i], bim->value_lens[i]);
            if (exists && bim->src != md->rank) {
                ::operator delete(bim->values[i]);
                bim->values[i] = nullptr;
            }
        } else {
            new_value = bim->values[i];
            new_value_len = bim->value_lens[i];
        }

        if (old_value) {
            // old_value comes from leveldb
            free(old_value);
            old_value = nullptr;
        }

        //Put the record in the database
        if ((ret =
             index->mdhim_stores[bim->rs_idx[i]]->put(index->mdhim_stores[bim->rs_idx[i]]->db_handle,
                                                      bim->keys[i], bim->key_lens[i],
                                                      new_value, new_value_len)) != MDHIM_SUCCESS) {
            mlog(MDHIM_SERVER_CRIT, "Rank %d - Error batch putting records",
                 md->rank);
            error = ret;
        } else {
            num_put = bim->num_keys;
        }

        //Update the stats if this key didn't exist before
        if (!exists && error == MDHIM_SUCCESS) {
            update_stat(md, index, bim->rs_idx[i], bim->keys[i], bim->key_lens[i]);
        }

        if (exists && md->p->db_opts->value_append == MDHIM_DB_APPEND) {
            //Release the value created for appending the new and old value
            ::operator delete(new_value);
        }
    }

  done:

    //Create the response message
    TransportBRecvMessage *brm = new TransportBRecvMessage();
    brm->src = bim->dst;
    brm->dst = bim->src;
    brm->error = error;
    brm->num_keys = bim->num_keys;
    brm->rs_idx = std::move(bim->rs_idx);

    //Release the bput keys/values if the message isn't coming from myself
    if (bim->src != md->rank) {
        for (std::size_t i = 0; i < bim->num_keys; i++) {
            ::operator delete(bim->keys[i]);
            bim->keys[i] = nullptr;
            ::operator delete(bim->values[i]);
            bim->values[i] = nullptr;
        }
    }

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
         index->mdhim_stores[dm->rs_idx]->del(index->mdhim_stores[dm->rs_idx]->db_handle,
                                              dm->key, dm->key_len)) != MDHIM_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error deleting record",
             md->rank);
    }

  done:
    //Create the response message
    TransportRecvMessage *rm = new TransportRecvMessage();
    rm->error = ret;
    rm->src = dm->dst;
    rm->dst = dm->src;
    rm->rs_idx = dm->rs_idx;

    // if the key came from a different rank, deallocate it
    if (md->rank != dm->src) {
        ::operator delete(dm->key);
    }

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
 * @return          MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int range_server_bdel(mdhim_t *md, work_item_t *item) {
    int error = 0;

    TransportBDeleteMessage *bdm = dynamic_cast<TransportBDeleteMessage *>(item->message);

    //Get the index referenced the message
    index_t *index = find_index(md, (TransportMessage *)bdm);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error retrieving index for id: %d",
             md->rank, bdm->index);
        error = MDHIM_ERROR;
        goto done;
    }

    //Iterate through the arrays and delete each record
    for (std::size_t i = 0; i < bdm->num_keys && i < MAX_BULK_OPS; i++) {
        //Put the record in the database
        int ret;
        if ((ret =
             index->mdhim_stores[bdm->rs_idx[i]]->del(index->mdhim_stores[bdm->rs_idx[i]]->db_handle,
                                                      bdm->keys[i], bdm->key_lens[i]))
            != MDHIM_SUCCESS) {
            mlog(MDHIM_SERVER_CRIT, "Rank %d - Error deleting record",
                 md->rank);
            error = ret;
        }
    }

  done:
    //Create the response message
    TransportBRecvMessage *brm = new TransportBRecvMessage();
    brm->error = error;
    brm->src = bdm->dst;
    brm->dst = bdm->src;
    brm->rs_idx = std::move(bdm->rs_idx);

    //Release the bdel keys if the message isn't coming from myself
    if (bdm->src != md->rank) {
        for (std::size_t i = 0; i < bdm->num_keys; i++) {
            ::operator delete(bdm->keys[i]);
            bdm->keys[i] = nullptr;
        }
    }

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

    //Get the index referenced the message
    index = find_index(md, im);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error retrieving index for id: %d",
             md->rank, im->index);
        ret = MDHIM_ERROR;
        goto done;
    }

    //Put the record in the database
    for(int i = 0; i < md->p->db_opts->dbs_per_server; i++) {
        if ((ret =
             index->mdhim_stores[i]->commit(index->mdhim_stores[i]->db_handle))
            != MDHIM_SUCCESS) {
            mlog(MDHIM_SERVER_CRIT, "Rank %d - Error committing database",
                 md->rank);
        }
    }

  done:
    //Create the response message
    TransportBRecvMessage *brm = new TransportBRecvMessage();
    brm->error = ret;
    brm->src = im->dst;
    brm->dst = im->src;
    for(int i = 0; i < md->p->db_opts->dbs_per_server; i++) {
        brm->rs_idx.push_back(i);
    }

    //Send response
    return send_locally_or_remote(md, item, brm);
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
    std::size_t value_len = 0;
    int error = 0;

    TransportGetMessage *gm = dynamic_cast<TransportGetMessage *>(item->message);

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
                 index->mdhim_stores[gm->rs_idx]->get(index->mdhim_stores[gm->rs_idx]->db_handle,
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
                 index->mdhim_stores[gm->rs_idx]->get_next(index->mdhim_stores[gm->rs_idx]->db_handle,
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
                 index->mdhim_stores[gm->rs_idx]->get_prev(index->mdhim_stores[gm->rs_idx]->db_handle,
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
                 index->mdhim_stores[gm->rs_idx]->get_next(index->mdhim_stores[gm->rs_idx]->db_handle,
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
                 index->mdhim_stores[gm->rs_idx]->get_prev(index->mdhim_stores[gm->rs_idx]->db_handle,
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

  done:
    //Create the response message
    TransportGetRecvMessage *grm = new TransportGetRecvMessage();
    grm->error = error;
    grm->src = gm->dst;
    grm->dst = gm->src;
    grm->rs_idx = gm->rs_idx;

    //If this message is coming from myself, copy the key so that the user still has ownership of the key
    if (gm->src == md->rank) {
        _clone(gm->key, gm->key_len, &grm->key);
    }
    //If this message was unpacked, do not delete gm->key, since it is reused in grm
    else {
        grm->key = gm->key;
    }

    grm->key_len = gm->key_len;

    grm->value = value;
    grm->value_len = value_len;

    grm->index = index->id;
    grm->index_type = index->type;

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

    TransportBGetMessage *bgm = dynamic_cast<TransportBGetMessage *>(item->message);

    void **values = new void *[bgm->num_keys]();
    std::size_t *value_lens = new std::size_t[bgm->num_keys]();

    //Get the index referenced the message
    index_t *index = find_index(md, (TransportMessage *) bgm);
    if (!index) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error retrieving index for id: %d",
             md->rank, bgm->index);
        error = MDHIM_ERROR;
        goto done;
    }

    //Iterate through the arrays and get each record
    for (std::size_t i = 0; i < bgm->num_keys && i < MAX_BULK_OPS; i++) {
        switch(bgm->op) {
            // Gets the value for the given key
            case TransportGetMessageOp::GET_EQ:
                if ((ret =
                     index->mdhim_stores[bgm->rs_idx[i]]->get(index->mdhim_stores[bgm->rs_idx[i]]->db_handle,
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
                     index->mdhim_stores[bgm->rs_idx[i]]->get_next(index->mdhim_stores[bgm->rs_idx[i]]->db_handle,
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
                     index->mdhim_stores[bgm->rs_idx[i]]->get_prev(index->mdhim_stores[bgm->rs_idx[i]]->db_handle,
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
                     index->mdhim_stores[bgm->rs_idx[i]]->get_next(index->mdhim_stores[bgm->rs_idx[i]]->db_handle,
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
                     index->mdhim_stores[bgm->rs_idx[i]]->get_prev(index->mdhim_stores[bgm->rs_idx[i]]->db_handle,
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
    }

  done:

    //Create the response message
    TransportBGetRecvMessage *bgrm = new TransportBGetRecvMessage();
    bgrm->error = error;
    bgrm->src = bgm->dst;
    bgrm->dst = bgm->src;
    bgrm->rs_idx = std::move(bgm->rs_idx);
    bgrm->num_keys = bgm->num_keys;
    bgrm->index = index?index->id:-1;
    bgrm->index_type = index?index->type:BAD_INDEX;

    // copy the values
    bgrm->values.resize(bgm->num_keys);
    bgrm->value_lens.resize(bgm->num_keys);
    for (std::size_t i = 0; i < bgm->num_keys; i++) {
        bgrm->values[i] = values[i];
        bgrm->value_lens[i] = value_lens[i];
    }

    delete [] values;
    delete [] value_lens;

    //If this message is coming from myself, copy the key so that the user still has ownership of the key
    if (bgm->src == md->rank) {
        bgrm->keys.resize(bgm->num_keys);
        for (std::size_t i = 0; i < bgm->num_keys; i++) {
            _clone(bgm->keys[i], bgm->key_lens[i], &bgrm->keys[i]);
            bgm->keys[i] = nullptr;
        }
    }
    //If this message was unpacked, do not delete bgm->keys, since it is reused in bgrm
    else {
        bgrm->keys = std::move(bgm->keys);
    }

    bgrm->key_lens = std::move(bgm->key_lens);

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

    TransportBGetMessage *bgm = dynamic_cast<TransportBGetMessage *>(item->message);

    const int rs_idx = bgm->dst % md->p->db_opts->dbs_per_server;

    //Initialize pointers and lengths
    const std::size_t count = bgm->num_keys * bgm->num_recs;
    std::deque<void *> values(count);
    std::deque<std::size_t> value_lens(count);

    std::deque<void *> keys(count);
    std::deque<std::size_t> key_lens(count);

    //Used for passing the key and key len to the db
    void ** get_key = new void *();
    std::size_t *get_key_len = new std::size_t();

    //Used for passing the value and value len to the db
    void ** get_value = new void *();
    std::size_t *get_value_len = new std::size_t();

    std::size_t num_records = 0;

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

    mlog(MDHIM_SERVER_CRIT, "Rank %d - Num keys is: %zu and num recs is: %zu",
         md->rank, bgm->num_keys, bgm->num_recs);
    //Iterate through the arrays and get each record
    for (std::size_t i = 0; i < bgm->num_keys; i++) {
        for (int j = 0; j < bgm->num_recs; j++) {
            keys[num_records] = nullptr;
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
                        keys[num_records] = nullptr;
                        key_lens[num_records] = sizeof(int32_t);
                    }
                case TransportGetMessageOp::GET_NEXT:
                    if (j && (ret =
                              index->mdhim_stores[rs_idx]->get_next(index->mdhim_stores[rs_idx]->db_handle,
                                                                   get_key, get_key_len,
                                                                   get_value, get_value_len))
                        != MDHIM_SUCCESS) {
                        mlog(MDHIM_SERVER_DBG, "Rank %d - Couldn't get next record",
                             md->rank);
                        error = ret;
                        key_lens[num_records] = 0;
                        value_lens[num_records] = 0;
                        goto respond;
                    } else if (!j && (ret =
                                      index->mdhim_stores[rs_idx]->get(index->mdhim_stores[rs_idx]->db_handle,
                                                                       *get_key, *get_key_len,
                                                                       get_value, get_value_len))
                               != MDHIM_SUCCESS) {
                        error = ret;
                        key_lens[num_records] = 0;
                        value_lens[num_records] = 0;
                        goto respond;
                    }
                    break;
                case TransportGetMessageOp::GET_LAST:
                    if (j == 0) {
                        keys[num_records] = nullptr;
                        key_lens[num_records] = sizeof(std::size_t);
                    }
                case TransportGetMessageOp::GET_PREV:
                    if (j && (ret =
                              index->mdhim_stores[rs_idx]->get_prev(index->mdhim_stores[rs_idx]->db_handle,
                                                                   get_key, get_key_len,
                                                                   get_value, get_value_len))
                        != MDHIM_SUCCESS) {
                        mlog(MDHIM_SERVER_DBG, "Rank %d - Couldn't get prev record",
                             md->rank);
                        error = ret;
                        key_lens[num_records] = 0;
                        value_lens[num_records] = 0;
                        goto respond;
                    } else if (!j && (ret =
                                      index->mdhim_stores[rs_idx]->get(index->mdhim_stores[rs_idx]->db_handle,
                                                                      *get_key, *get_key_len,
                                                                       get_value, get_value_len))
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
    //Create the response message
    TransportBGetRecvMessage *bgrm = new TransportBGetRecvMessage();
    bgrm->error = error;
    bgrm->src = bgm->dst;
    bgrm->dst = bgm->src;
    bgrm->rs_idx = std::move(bgm->rs_idx);
    bgrm->keys = keys;
    bgrm->key_lens = key_lens;
    bgrm->values = values;
    bgrm->value_lens = value_lens;
    bgrm->num_keys = num_records;
    bgrm->index = index->id;
    bgrm->index_type = index->type;

    //If this message is coming from myself, copy the key so that the user still has ownership of the key
    if (bgm->src == md->rank) {
        bgrm->keys.resize(bgm->num_keys);
        for (std::size_t i = 0; i < bgm->num_keys; i++) {
            _clone(bgm->keys[i], bgm->key_lens[i], &bgrm->keys[i]);
        }
    }
    //If this message was unpacked, do not delete bgm->keys, since it is reused in bgrm
    else {
        bgrm->keys = std::move(bgm->keys);
        bgm->keys.clear();
    }

    bgrm->key_lens = std::move(bgm->key_lens);

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

    while (!md->p->shutdown) {
        //Lock the work queue mutex
        pthread_mutex_lock(&md->p->mdhim_rs->work_queue_mutex);
        pthread_cleanup_push((void (*)(void *)) pthread_mutex_unlock,
                             (void *) &md->p->mdhim_rs->work_queue_mutex);

        //Wait until there is work to be performed
        //Do not loop
        if (!(item = get_work(md))) {
            pthread_cond_wait(&md->p->mdhim_rs->work_ready_cv, &md->p->mdhim_rs->work_queue_mutex);
            item = get_work(md);
        }

        pthread_cleanup_pop(0);
        pthread_mutex_unlock(&md->p->mdhim_rs->work_queue_mutex);

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

            // go to the next item
            work_item_t *next = item->next;
            delete item;
            item = next;
        }

        //Clean outstanding sends
        range_server_clean_oreqs(md);
    }

    return nullptr;
}

int range_server_add_oreq(mdhim_t *md, MPI_Request *req, void *msg) {
    out_req_t *oreq;
    out_req_t *item;

    pthread_mutex_lock(&md->p->mdhim_rs->out_req_mutex);
    item = md->p->mdhim_rs->out_req_list;
    oreq = new out_req_t();
    oreq->next = nullptr;
    oreq->prev = nullptr;
    oreq->message = msg;
    oreq->req = req;

    if (!item) {
        md->p->mdhim_rs->out_req_list = oreq;
        pthread_mutex_unlock(&md->p->mdhim_rs->out_req_mutex);
        return MDHIM_SUCCESS;
    }

    item->prev = oreq;
    oreq->next = item;
    md->p->mdhim_rs->out_req_list = oreq;
    pthread_mutex_unlock(&md->p->mdhim_rs->out_req_mutex);

    return MDHIM_SUCCESS;
}

static out_req_t *clean_item(out_req_t *item) {
    if (!item) {
        return nullptr;
    }

    out_req_t *next = item->next;

    ::operator delete(item->message);
    delete item->req;
    delete item;

    return next;
}

int range_server_clean_oreqs(mdhim_t *md) {
    pthread_mutex_lock(&md->p->mdhim_rs->out_req_mutex);
    out_req_t *item = md->p->mdhim_rs->out_req_list;
    while (item) {
        if (!item->req) {
            item = clean_item(item);
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
                item->next->prev = nullptr;
            }
        } else {
            if (item->next) {
                item->next->prev = item->prev;
            }
            if (item->prev) {
                item->prev->next = item->next;
            }
        }

        item = clean_item(item);
    }

    pthread_mutex_unlock(&md->p->mdhim_rs->out_req_mutex);

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

    //Initialize work queue
    md->p->mdhim_rs->work_queue = new work_queue_t();
    md->p->mdhim_rs->work_queue->head = nullptr;
    md->p->mdhim_rs->work_queue->tail = nullptr;

    //Initialize the outstanding request list
    md->p->mdhim_rs->out_req_list = nullptr;

    //Initialize work queue mutex
    md->p->mdhim_rs->work_queue_mutex = PTHREAD_MUTEX_INITIALIZER;

    //Initialize out req mutex
    md->p->mdhim_rs->out_req_mutex = PTHREAD_MUTEX_INITIALIZER;

    //Initialize the condition variables
    md->p->mdhim_rs->work_ready_cv = PTHREAD_COND_INITIALIZER;

    //Initialize worker threads
    md->p->mdhim_rs->workers = new pthread_t *[md->p->db_opts->num_wthreads]();
    for (std::size_t i = 0; i < md->p->db_opts->num_wthreads; i++) {
        md->p->mdhim_rs->workers[i] = new pthread_t();
        if ((ret = pthread_create(md->p->mdhim_rs->workers[i], nullptr,
                                  worker_thread, (void *) md)) != 0) {
            mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
                 "Error while initializing worker thread",
                 md->rank);
            return MDHIM_ERROR;
        }
    }

    return MDHIM_SUCCESS;
}
