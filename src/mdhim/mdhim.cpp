/*
 * MDHIM TNG
 *
 * MDHIM API implementation
 */

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sys/time.h>

#include "data_store.h"
#include "indexes.h"
#include "local_client.h"
#include "mdhim.h"
#include "mdhim_options.h"
#include "mdhim_options_private.h"
#include "mdhim_private.h"
#include "partitioner.h"
#include "range_server.h"
#include "transport_private.hpp"

/**
 * groupInitialization
 * Initializes MPI values in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int groupInitialization(mdhim_t *md) {
    if (!md || !md->p){
        return MDHIM_ERROR;
    }

    // TODO: remove this block//////////////////////
    //Don't allow MPI_COMM_NULL to be the communicator
    md->p->mdhim_client_comm = MPI_COMM_NULL;
    if (md->p->mdhim_comm == MPI_COMM_NULL) {
        md->p->mdhim_client_comm = MPI_COMM_NULL;
        return MDHIM_ERROR;
    }

    //Dup the communicator passed in for barriers between clients
    if (MPI_Comm_dup(md->p->mdhim_comm, &md->p->mdhim_client_comm) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }

    //Get the size of the main MDHIM communicator
    if (MPI_Comm_size(md->p->mdhim_comm, &md->p->mdhim_comm_size) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }

    //Get the rank of the main MDHIM communicator
    if (MPI_Comm_rank(md->p->mdhim_comm, &md->p->mdhim_rank) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }
    // /////////////////////////////////////////////

    return MDHIM_SUCCESS;
}

/**
 * groupDestruction
 * Cleans up MPI values in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int groupDestruction(mdhim_t *md) {
    if (!md || !md->p){
        return MDHIM_ERROR;
    }

    //Destroy the client communicator
    if (md->p->mdhim_client_comm != MPI_COMM_NULL) {
        MPI_Comm_free(&md->p->mdhim_client_comm);
    }

    return MDHIM_SUCCESS;
}

/**
 * indexnitialization
 * Initializes index values in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int indexInitialization(mdhim_t *md) {
    if (!md || !md->p){
        return MDHIM_ERROR;
    }

    //Initialize the indexes and create the primary index
    md->p->indexes = nullptr;
    md->p->indexes_by_name = nullptr;
    if (pthread_rwlock_init(&md->p->indexes_lock, nullptr) != 0) {
        return MDHIM_ERROR;
    }

    //Create the default remote primary index
    md->p->primary_index = create_global_index(md, md->p->db_opts->p->rserver_factor, md->p->db_opts->p->max_recs_per_slice,
                                               md->p->db_opts->p->db_type, md->p->db_opts->p->db_key_type, nullptr);

    if (!md->p->primary_index) {
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * indexDestruction
 * Cleans up index values in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int indexDestruction(mdhim_t *md) {
    if (!md || !md->p){
        return MDHIM_ERROR;
    }

    if (pthread_rwlock_destroy(&md->p->indexes_lock) != 0) {
        return MDHIM_ERROR;
    }

    indexes_release(md);

    return MDHIM_SUCCESS;
}

/**
 * mdhimInit
 * Initializes MDHIM
 *
 * @param md MDHIM context
 * @param opts Options structure for DB creation, such as name, and primary key type
 * @return MDHIM status value
 */
int mdhimInit(mdhim_t* md, mdhim_options_t *opts) {
    if (!md){
      return MDHIM_ERROR;
    }

    if (!opts || !opts->p){
      return MDHIM_ERROR;
    }

    //Open mlog - stolen from plfs
    mlog_open((char *)"mdhim", 0, opts->p->debug_level, opts->p->debug_level, nullptr, 0, MLOG_LOGPID, 0);

    //Initialize context variables based on options
    if (!(md->p = new mdhim_private_t())) {
        return MDHIM_ERROR;
    }

    md->p->mdhim_comm = opts->p->comm;
    md->p->mdhim_comm_lock = PTHREAD_MUTEX_INITIALIZER;

    //Initialize group members of context
    if (groupInitialization(md) != MDHIM_SUCCESS){
        mlog(MDHIM_CLIENT_CRIT, "MDHIM - Error Group Initialization Failed");
        return MDHIM_ERROR;
    }

    // initialize the transport
    if (mdhim_private_init(md->p, opts->p->db_type, opts->p->transporttype) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    //Required for index initialization
    md->p->mdhim_rs = nullptr;
    md->p->db_opts = opts;

    //Initialize the partitioner
    partitioner_init();

    //Initialize index members of context
    if (indexInitialization(md) != MDHIM_SUCCESS){
        mlog(MDHIM_CLIENT_CRIT, "MDHIM - Error Index Initialization Failed");
        return MDHIM_ERROR;
    }

    //Flag that won't be used until shutdown
    md->p->shutdown = 0;

    md->p->receive_msg_mutex = PTHREAD_MUTEX_INITIALIZER;
    md->p->receive_msg_ready_cv = PTHREAD_COND_INITIALIZER;
    md->p->receive_msg = nullptr;

    return MDHIM_SUCCESS;
}

/**
 * Quits the MDHIM instance
 *
 * @param md MDHIM context to be closed
 * @return MDHIM status value
 */
int mdhimClose(struct mdhim *md) {
    if (!md || !md->p) {
        return MDHIM_ERROR;
    }

    //Force shutdown if not already started
    md->p->shutdown = 1;

    //Stop range server if I'm a range server
    if (md->p->mdhim_rs) {
        range_server_stop(md);
    }

    //Free up memory used by the partitioner
    partitioner_release();

    //Free up memory used by indexes
    indexDestruction(md);

    //Free up memory used by message buffer
    ::operator delete(md->p->receive_msg);
    md->p->receive_msg = nullptr;

    //Clean up group members
    groupDestruction(md);

    delete md->p->transport;
    md->p->transport = nullptr;

    delete md->p;
    md->p = nullptr;

    //Close MLog
    mlog_close();

    return MDHIM_SUCCESS;
}

/**
 * Commits outstanding MDHIM writes - collective call
 *
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimCommit(struct mdhim *md, index_t *index) {
    int ret = MDHIM_SUCCESS;

    if (!index) {
        index = md->p->primary_index;
    }

    //If I'm a range server, send a commit message to myself
    if (im_range_server(index)) {
        TransportRecvMessage *cm = new TransportRecvMessage();
        cm->mtype = TransportMessageType::COMMIT;
        cm->index = index->id;
        cm->index_type = index->type;
        cm->dst =  md->p->mdhim_rank;
        TransportRecvMessage *rm = local_client_commit(md, static_cast<TransportMessage *>(cm));
        if (!rm || rm->error) {
            ret = MDHIM_ERROR;
            mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
                 "Error while committing database in mdhimTransportit",
                  md->p->mdhim_rank);
        }

        delete rm;
    }

    return ret;
}

/**
 * Inserts a single record into MDHIM
 *
 * @param md main MDHIM context
 * @param primary_key        pointer to key to store
 * @param primary_key_len    the length of the key
 * @param value              pointer to the value to store
 * @param value_len          the length of the value
 * @param secondary_info     secondary global and local information for
                             inserting secondary global and local keys
 * @return                   mdhim_brm_t * or nullptr on error
 */
mdhim_brm_t *mdhimPut(struct mdhim *md,
                      void *primary_key, int primary_key_len,
                      void *value, int value_len,
                      secondary_info_t *secondary_global_info,
                      secondary_info_t *secondary_local_info) {
    if (!md || !md->p ||
        !primary_key || !primary_key_len ||
        !value || !value_len) {
        return nullptr;
    }

    // Clone primary key and value
    void *pk = ::operator new(primary_key_len);
    void *val = ::operator new(value_len);
    if (!pk || !val) {
        ::operator delete(pk);
        ::operator delete(val);
        return nullptr;
    }

    memcpy(pk, primary_key, primary_key_len);
    memcpy(val, value, value_len);

    //Send the primary key and value
    TransportRecvMessage *rm = _put_record(md, md->p->primary_index, pk, primary_key_len, val, value_len);
    if (!rm) {
        return nullptr;
    }

    TransportBRecvMessage *head = _create_brm(rm);
    delete(rm);

    //Return message from each _put_record call
    TransportBRecvMessage *brm = nullptr;

    //Insert the secondary local key if it was given
    if (secondary_local_info && secondary_local_info->secondary_index &&
        secondary_local_info->secondary_keys &&
        secondary_local_info->secondary_key_lens &&
        secondary_local_info->num_keys) {
        void **primary_keys = new void *[secondary_local_info->num_keys]();
        int *primary_key_lens = new int[secondary_global_info->num_keys]();
        for (int i = 0; i < secondary_local_info->num_keys; i++) {
            primary_keys[i] = ::operator new(primary_key_len);
            memcpy(primary_keys[i], primary_key, primary_key_len);
            primary_key_lens[i] = primary_key_len;
        }

        brm = _bput_records(md,
                            secondary_local_info->secondary_index,
                            secondary_local_info->secondary_keys,
                            secondary_local_info->secondary_key_lens,
                            primary_keys, primary_key_lens,
                            secondary_local_info->num_keys);
        delete [] primary_keys;
        delete [] primary_key_lens;
        if (!brm) {
            return mdhim_brm_init(head);
        }

        _concat_brm(head, brm);
    }

    //Insert the secondary global key if it was given
    if (secondary_global_info && secondary_global_info->secondary_index &&
        secondary_global_info->secondary_keys &&
        secondary_global_info->secondary_key_lens &&
        secondary_global_info->num_keys) {
        void **primary_keys = new void *[secondary_global_info->num_keys]();
        int *primary_key_lens = new int[secondary_global_info->num_keys]();
        for (int i = 0; i < secondary_global_info->num_keys; i++) {
            primary_keys[i] = ::operator new(primary_key_len);
            memcpy(primary_keys[i], primary_key, primary_key_len);
            primary_key_lens[i] = primary_key_len;
        }
        brm = _bput_records(md,
                            secondary_global_info->secondary_index,
                            secondary_global_info->secondary_keys,
                            secondary_global_info->secondary_key_lens,
                            primary_keys, primary_key_lens,
                            secondary_global_info->num_keys);

        delete [] primary_keys;
        delete [] primary_key_lens;
        if (!brm) {
            return mdhim_brm_init(head);
        }

        _concat_brm(head, brm);
    }

    return mdhim_brm_init(head);
}

/**
 * Inserts a single record into an MDHIM secondary index
 *
 * @param md                  main MDHIM struct
 * @param secondary_index     where to store the secondary_key
 * @param secondary_key       pointer to key to store
 * @param secondary_key_len   the length of the key
 * @param primary_key         pointer to the primary_key
 * @param primary_key_len     the length of the value
 * @return mdhim_brm_t * or nullptr on error
 */
mdhim_brm_t *mdhimPutSecondary(mdhim_t *md,
                               index_t *secondary_index,
                               void *secondary_key, int secondary_key_len,
                               void *primary_key, int primary_key_len) {
    if (!md || !md->p ||
        !secondary_key || !secondary_key_len ||
        !primary_key || !primary_key_len) {
        return nullptr;
    }

    TransportRecvMessage *rm = _put_record(md, secondary_index,
                                           secondary_key, secondary_key_len,
                                           primary_key, primary_key_len);
    if (!rm) {
        return nullptr;
    }

    TransportBRecvMessage *head = _create_brm(rm);
    ::operator delete(rm);

    return mdhim_brm_init(head);
}

TransportBRecvMessage *_bput_secondary_keys_from_info(mdhim_t *md,
                                                      secondary_bulk_info_t *secondary_info,
                                                      void **primary_keys, int *primary_key_lens,
                                                      int num_records) {
    if (!md || !md->p ||
        !secondary_info ||
        !primary_keys || !primary_key_lens) {
        return nullptr;
    }

    TransportBRecvMessage *head =  nullptr;
    for (int i = 0; i < num_records; i++) {
        void **primary_keys_to_send = new void *[secondary_info->num_keys[i]]();
        int *primary_key_lens_to_send = new int[secondary_info->num_keys[i]]();

        // copy the given keys
        for (int j = 0; j < secondary_info->num_keys[i]; j++) {
            primary_keys_to_send[j] = ::operator new(primary_key_lens[i]);
            memcpy(primary_keys_to_send[j], primary_keys[i], primary_key_lens[i]);
            primary_key_lens_to_send[j] = primary_key_lens[i];
        }

        TransportBRecvMessage *newone = _bput_records(md, secondary_info->secondary_index,
                                                      secondary_info->secondary_keys[i],
                                                      secondary_info->secondary_key_lens[i],
                                                      primary_keys_to_send, primary_key_lens_to_send,
                                                      secondary_info->num_keys[i]);
        if (!head) {
            head = newone;
        } else if (newone) {
            _concat_brm(head, newone);
        }

        delete [] primary_keys_to_send;
        delete [] primary_key_lens_to_send;
    }

    return head;
}

/**
 * Inserts multiple records into MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to store
 * @param key_lens     array with lengths of each key in keys
 * @param values       pointer to array of values to store
 * @param value_lens   array with lengths of each value
 * @param num_records  the number of records to store (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or nullptr on error
 */
mdhim_brm_t *mdhimBPut(mdhim_t *md,
                       void **primary_keys, int *primary_key_lens,
                       void **primary_values, int *primary_value_lens,
                       int num_records,
                       secondary_bulk_info_t *secondary_global_info,
                       secondary_bulk_info_t *secondary_local_info) {
    if (!md || !md->p ||
        !primary_keys || !primary_key_lens ||
        !primary_values || !primary_value_lens) {
        return nullptr;
    }

    TransportBRecvMessage *head = _bput_records(md, md->p->primary_index, primary_keys, primary_key_lens,
                                                primary_values, primary_value_lens, num_records);

    if (!head || head->error) {
        return mdhim_brm_init(head);
    }

    //Insert the secondary local keys if they were given
    if (secondary_local_info && secondary_local_info->secondary_index &&
        secondary_local_info->secondary_keys &&
        secondary_local_info->secondary_key_lens) {
        TransportBRecvMessage *newone = _bput_secondary_keys_from_info(md, secondary_local_info, primary_keys,
                                                                       primary_key_lens, num_records);
        if (newone) {
            _concat_brm(head, newone);
        }
    }

    //Insert the secondary global keys if they were given
    if (secondary_global_info && secondary_global_info->secondary_index &&
        secondary_global_info->secondary_keys &&
        secondary_global_info->secondary_key_lens) {
        TransportBRecvMessage *newone = _bput_secondary_keys_from_info(md, secondary_global_info, primary_keys,
                                                                       primary_key_lens, num_records);
        if (newone) {
            _concat_brm(head, newone);
        }
    }

    return mdhim_brm_init(head);
}

/**
 * Inserts multiple records into an MDHIM secondary index
 *
 * @param md           main MDHIM struct
 * @param index        the secondary index to use
 * @param keys         pointer to array of keys to store
 * @param key_lens     array with lengths of each key in keys
 * @param values       pointer to array of values to store
 * @param value_lens   array with lengths of each value
 * @param num_records  the number of records to store (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or nullptr on error
 */
mdhim_brm_t *mdhimBPutSecondary(mdhim_t *md, index_t *secondary_index,
                                void **secondary_keys, int *secondary_key_lens,
                                void **primary_keys, int *primary_key_lens,
                                int num_records) {
    if (!md || !md->p ||
        !secondary_keys || !secondary_key_lens ||
        !primary_keys || !primary_key_lens) {
        return nullptr;
    }

    TransportBRecvMessage *brm = _bput_records(md, secondary_index,
                                               secondary_keys, secondary_key_lens,
                                               primary_keys, primary_key_lens,
                                               num_records);
    return mdhim_brm_init(brm);
}

/**
 * Retrieves a single record from MDHIM
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to get value of or last key to start from if op is
 (MDHIM_GET_NEXT or MDHIM_GET_PREV)
 * @param key_len   the length of the key
 * @param op        the operation type
 * @return mdhim_getrm_t * or nullptr on error
 */
mdhim_getrm_t *mdhimGet(mdhim_t *md, index_t *index,
                         void *key, int key_len,
                         enum TransportGetMessageOp op) {
    if (!md || !md->p ||
        index ||
        !key || !key_len) {
        return nullptr;
    }

    if (op != TransportGetMessageOp::GET_EQ && op != TransportGetMessageOp::GET_PRIMARY_EQ) {
        return nullptr;
    }

    // Clone primary key and value
    void *k = ::operator new(key_len);
    if (!k) {
        return nullptr;
    }
    memcpy(k, key, key_len);

    if (!index) {
        index = md->p->primary_index;
    }

    return mdhim_grm_init(_get_record(md, index, k, key_len, op));
}

/**
 * Retrieves multiple records from MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to get values for
 * @param key_lens     array with lengths of each key in keys
 * @param num_records  the number of keys to get (i.e., the number of keys in keys array)
 * @return mdhim_bgetrm_t * or nullptr on error
 */
mdhim_bgetrm_t *mdhimBGet(mdhim_t *md, index_t *index,
                          void **keys, int *key_lens,
                          int num_keys, enum TransportGetMessageOp op) {
    if (op != TransportGetMessageOp::GET_EQ && op != TransportGetMessageOp::GET_PRIMARY_EQ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Invalid operation for mdhimBGet",
              md->p->mdhim_rank);
        return nullptr;
    }

    //Check to see that we were given a sane amount of records
    if (num_keys > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Too many bulk operations requested in mdhimBGet",
              md->p->mdhim_rank);
        return nullptr;
    }

    if (!index) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Invalid index specified",
              md->p->mdhim_rank);
        return nullptr;
    }

    TransportBGetRecvMessage *bgrm_head = _bget_records(md, index, keys, key_lens, num_keys, 1, op);
    if (!bgrm_head) {
        return nullptr;
    }

    if (op == TransportGetMessageOp::GET_PRIMARY_EQ) {
        //Get the number of keys/values we received
        int plen = 0;
        while (bgrm_head) {
            for(int i = 0; i < bgrm_head->num_keys; i++) {
                plen++;
            }

            bgrm_head = bgrm_head->next;
        }

        if (plen > MAX_BULK_OPS) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                 "Too many bulk operations would be performed "
                 "with the MDHIM_GET_PRIMARY_EQ operation.  Limiting "
                 "request to : %u key/values",
                  md->p->mdhim_rank, MAX_BULK_OPS);
            plen = MAX_BULK_OPS - 1;
        }

        void **primary_keys = new void *[plen]();
        int *primary_key_lens = new int[plen]();

        //Get the primary keys from the previously received messages' values
        plen = 0;
        while (bgrm_head) {
            for(int i = 0; i < bgrm_head->num_keys && plen < MAX_BULK_OPS; i++) {
                primary_keys[plen] = ::operator new(bgrm_head->value_lens[i]);
                memcpy(primary_keys[plen], bgrm_head->values[i], bgrm_head->value_lens[i]);
                primary_key_lens[plen] = bgrm_head->value_lens[i];
                plen++;
            }

            TransportBGetRecvMessage *next = bgrm_head->next;
            delete bgrm_head;
            bgrm_head = next;
        }

        index_t *primary_index = get_index(md, index->primary_id);

        //Get the primary keys' values
        bgrm_head = _bget_records(md, primary_index,
                                  primary_keys, primary_key_lens,
                                  plen, 1, TransportGetMessageOp::GET_EQ);

        //Free up the primary keys and lens arrays
        for (int i = 0; i < plen; i++) {
            ::operator delete(primary_keys[i]);
        }

        delete [] primary_keys;
        delete [] primary_key_lens;
    }

    //Return the head of the list
    return mdhim_bgrm_init(bgrm_head);
}

/**
 * Retrieves multiple sequential records from a single range server if they exist
 *
 * If the operation passed in is MDHIM_GET_NEXT or MDHIM_GET_PREV, this return all the records
 * starting from the key passed in in the direction specified
 *
 * If the operation passed in is MDHIM_GET_FIRST and MDHIM_GET_LAST and the key is nullptr,
 * then this operation will return the keys starting from the first or last key
 *
 * If the operation passed in is MDHIM_GET_FIRST and MDHIM_GET_LAST and the key is not nullptr,
 * then this operation will return the keys starting the first key on
 * the range server that the key resolves to
 *
 * @param md           main MDHIM struct
 * @param key          pointer to the key to start getting next entries from
 * @param key_len      the length of the key
 * @param num_records  the number of successive keys to get
 * @param op           the operation to perform (i.e., MDHIM_GET_NEXT or MDHIM_GET_PREV)
 * @return mdhim_bgetrm_t * or nullptr on error
 */
mdhim_bgetrm_t *mdhimBGetOp(mdhim_t *md, index_t *index,
                            void *key, int key_len,
                            int num_records, enum TransportGetMessageOp op) {
    if (num_records > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "To many bulk operations requested in mdhimBGetOp",
              md->p->mdhim_rank);
        return nullptr;
    }

    if (op == TransportGetMessageOp::GET_EQ || op == TransportGetMessageOp::GET_PRIMARY_EQ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Invalid op specified for mdhimGet",
              md->p->mdhim_rank);
        return nullptr;
    }

    //Get the linked list of return messages from mdhimBGet
    return mdhim_bgrm_init( _bget_records(md, index, &key, &key_len, 1, num_records, op));
}

/**
 * Deletes a single record from MDHIM
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to delete
 * @param key_len   the length of the key
 * @return mdhim_rm_t * or nullptr on error
 */
mdhim_brm_t *mdhimDelete(mdhim_t *md, index_t *index,
                void *key, int key_len) {
    return mdhim_brm_init(_bdel_records(md, index, &key, &key_len, 1));
}

/**
 * Deletes multiple records from MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to delete
 * @param key_lens     array with lengths of each key in keys
 * @param num_records  the number of keys to delete (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or nullptr on error
 */
mdhim_brm_t *mdhimBDelete(mdhim_t *md, index_t *index,
                          void **keys, int *key_lens,
                          int num_records) {
    //Check to see that we were given a sane amount of records
    if (num_records > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "To many bulk operations requested in mdhimBGetOp",
              md->p->mdhim_rank);
        return nullptr;
    }

    return mdhim_brm_init(_bdel_records(md, index, keys, key_lens, num_records));
}

/**
 * Retrieves statistics from all the range servers - collective call
 *
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimStatFlush(mdhim_t *md, index_t *index) {
    int ret;

    MPI_Barrier(md->p->mdhim_client_comm);
    if ((ret = get_stat_flush(md, index)) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Error while getting MDHIM stat data in mdhimStatFlush",
              md->p->mdhim_rank);
    }
    MPI_Barrier(md->p->mdhim_client_comm);

    return ret;
}

/**
 * Sets the secondary_info structure used in mdhimPut
 *
 */
secondary_info_t *mdhimCreateSecondaryInfo(index_t *secondary_index,
                                           void **secondary_keys, int *secondary_key_lens,
                                           int num_keys, int info_type) {
    if (!secondary_index || !secondary_keys ||
        !secondary_key_lens || !num_keys) {
        return nullptr;
    }

    if (info_type != SECONDARY_GLOBAL_INFO &&
        info_type != SECONDARY_LOCAL_INFO) {
        return nullptr;
    }

    //Initialize the struct
    secondary_info_t *sinfo = new secondary_info_t();

    //Set the index fields
    sinfo->secondary_index = secondary_index;
    sinfo->secondary_keys = secondary_keys;
    sinfo->secondary_key_lens = secondary_key_lens;
    sinfo->num_keys = num_keys;
    sinfo->info_type = info_type;

    return sinfo;
}

void mdhimReleaseSecondaryInfo(secondary_info_t *si) {
    delete si;
}

/**
 * Sets the secondary_info structure used in mdhimBPut
 *
 */
secondary_bulk_info_t *mdhimCreateSecondaryBulkInfo(index_t *secondary_index,
                                                    void ***secondary_keys,
                                                    int **secondary_key_lens,
                                                    int *num_keys, int info_type) {
    if (!secondary_index || !secondary_keys ||
        !secondary_key_lens || !num_keys) {
        return nullptr;
    }

    if (info_type != SECONDARY_GLOBAL_INFO &&
        info_type != SECONDARY_LOCAL_INFO) {
        return nullptr;
    }

    //Initialize the struct
    secondary_bulk_info_t *sinfo = new secondary_bulk_info_t();

    //Set the index fields
    sinfo->secondary_index = secondary_index;
    sinfo->secondary_keys = secondary_keys;
    sinfo->secondary_key_lens = secondary_key_lens;
    sinfo->num_keys = num_keys;
    sinfo->info_type = info_type;

    return sinfo;
}

void mdhimReleaseSecondaryBulkInfo(secondary_bulk_info_t *si) {
    delete si;
}

/* what server would respond to this key? */
int mdhimWhichServer(mdhim_t *md, void *key, int key_len)
{
    return(_which_server(md, key, key_len));
}
