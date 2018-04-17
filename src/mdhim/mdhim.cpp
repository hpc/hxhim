/*
 * MDHIM TNG
 *
 * MDHIM API implementation
 */

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <pthread.h>
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
 * bootstrapInit
 * Initializes bootstrapping values in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int bootstrapInit(mdhim_t *md) {
    if (!md || !md->p){
        return MDHIM_ERROR;
    }

    md->mdhim_comm = MPI_COMM_WORLD;
    md->mdhim_comm_lock = PTHREAD_MUTEX_INITIALIZER;

    //Get the size of the main MDHIM communicator
    if (MPI_Comm_size(md->mdhim_comm, &md->mdhim_comm_size) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }

    //Get the rank of the main MDHIM communicator
    if (MPI_Comm_rank(md->mdhim_comm, &md->mdhim_rank) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * bootstrapDestroy
 * Cleans up MPI values in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int bootstrapDestroy(mdhim_t *md) {
    if (!md || !md->p){
        return MDHIM_ERROR;
    }

    md->mdhim_comm = MPI_COMM_NULL;
    md->mdhim_comm_size = 0;
    md->mdhim_rank = 0;

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
    if (!md ||
        !opts || !opts->p || !opts->p->transport || !opts->p->db){
        return MDHIM_ERROR;
    }

    //Open mlog - stolen from plfs
    mlog_open((char *)"mdhim", 0, opts->p->db->debug_level, opts->p->db->debug_level, nullptr, 0, MLOG_LOGPID, 0);

    if (!(md->p = new mdhim_private_t())) {
        return MDHIM_ERROR;
    }

    //Initialize bootstrapping variables
    if (bootstrapInit(md) != MDHIM_SUCCESS){
        mlog(MDHIM_CLIENT_CRIT, "MDHIM - Error Bootstrap Initialization Failed");
        return MDHIM_ERROR;
    }

    //Initialize context variables based on options
    if (mdhim_private_init(md, opts->p->db, opts->p->transport) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * Quits the MDHIM instance
 *
 * @param md MDHIM context to be closed
 * @return MDHIM status value
 */
int mdhimClose(mdhim_t *md) {
    mdhim_private_destroy(md);

    //Clean up bootstrapping variables
    bootstrapDestroy(md);

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
int mdhimCommit(mdhim_t *md, index_t *index) {
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
        cm->src = md->mdhim_rank;
        cm->dst = md->mdhim_rank;
        TransportRecvMessage *rm = local_client_commit(md, static_cast<TransportMessage *>(cm));
        if (!rm || rm->error) {
            ret = MDHIM_ERROR;
            mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
                 "Error while committing database in mdhimTransportit",
                  md->mdhim_rank);
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
mdhim_brm_t *mdhimPut(mdhim_t *md,
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
        int *primary_key_lens = new int[secondary_local_info->num_keys]();
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

    //Copy the keys and values
    void **pks = new void *[num_records]();
    int *pk_lens = new int[num_records]();
    void **pvs = new void *[num_records]();
    int *pv_lens = new int[num_records]();

    if (!pks || !pk_lens ||
        !pvs || !pv_lens) {
        delete [] pks;
        delete [] pk_lens;
        delete [] pvs;
        delete [] pv_lens;
        return nullptr;
    }

    for(int i = 0; i < num_records; i++) {
        if (!(pks[i] = ::operator new(primary_key_lens[i])) ||
            !(pvs[i] = ::operator new(primary_value_lens[i]))) {
            for(int j = 0; j < i; j++) {
                ::operator delete(pks[j]);
                ::operator delete(pvs[j]);
            }

            delete [] pks;
            delete [] pk_lens;
            delete [] pvs;
            delete [] pv_lens;
            return nullptr;
        }

        memcpy(pks[i], primary_keys[i], primary_key_lens[i]);
        pk_lens[i] = primary_key_lens[i];

        memcpy(pvs[i], primary_values[i], primary_value_lens[i]);
        pv_lens[i] = primary_value_lens[i];
    }

    TransportBRecvMessage *head = _bput_records(md, md->p->primary_index,
                                                pks, pk_lens,
                                                pvs, pv_lens,
                                                num_records);

    if (!head) {
        return nullptr;
    }

    //Insert the secondary local keys if they were given
    if (secondary_local_info && secondary_local_info->secondary_index &&
        secondary_local_info->secondary_keys &&
        secondary_local_info->secondary_key_lens) {

        TransportBRecvMessage *newone = _bput_secondary_keys_from_info(md, secondary_local_info,
                                                                       primary_keys, primary_key_lens,
                                                                       num_records);
        if (newone) {
            _concat_brm(head, newone);
        }
    }

    //Insert the secondary global keys if they were given
    if (secondary_global_info && secondary_global_info->secondary_index &&
        secondary_global_info->secondary_keys &&
        secondary_global_info->secondary_key_lens) {

        TransportBRecvMessage *newone = _bput_secondary_keys_from_info(md, secondary_global_info,
                                                                       primary_keys, primary_key_lens,
                                                                       num_records);
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
        !secondary_index ||
        !secondary_keys || !secondary_key_lens ||
        !primary_keys || !primary_key_lens) {
        return nullptr;
    }

    //Copy the secondary and primary keys
    void **sks = new void *[num_records]();
    int *sk_lens = new int[num_records]();
    void **pks = new void *[num_records]();
    int *pk_lens = new int[num_records]();

    if (!sks || !sk_lens ||
        !pks || !pk_lens) {
        delete [] sks;
        delete [] sk_lens;
        delete [] pks;
        delete [] pk_lens;
        return nullptr;
    }

    for(int i = 0; i < num_records; i++) {
        if (!(sks[i] = ::operator new(secondary_key_lens[i])) ||
            !(pks[i] = ::operator new(primary_key_lens[i]))) {
            for(int j = 0; j < i; j++) {
                ::operator delete(sks[j]);
                ::operator delete(pks[j]);
            }

            delete [] sks;
            delete [] sk_lens;
            delete [] pks;
            delete [] pk_lens;
            return nullptr;
        }

        memcpy(sks[i], secondary_keys[i], secondary_key_lens[i]);
        sk_lens[i] = secondary_key_lens[i];

        memcpy(pks[i], primary_keys[i], primary_key_lens[i]);
        pk_lens[i] = primary_key_lens[i];
    }

    return mdhim_brm_init(_bput_records(md, secondary_index,
                                        sks, sk_lens,
                                        pks, pk_lens,
                                        num_records));
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
        !key || !key_len) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    if (op != TransportGetMessageOp::GET_EQ && op != TransportGetMessageOp::GET_PRIMARY_EQ) {
        return nullptr;
    }

    // Clone primary key
    void *k = ::operator new(key_len);
    if (!k) {
        return nullptr;
    }
    memcpy(k, key, key_len);

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
    if (!md || !md->p ||
        !keys || !key_lens) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    if (op != TransportGetMessageOp::GET_EQ && op != TransportGetMessageOp::GET_PRIMARY_EQ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Invalid operation for mdhimBGet",
              md->mdhim_rank);
        return nullptr;
    }

    //Check to see that we were given a sane amount of records
    if (num_keys > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Too many bulk operations requested in mdhimBGet",
              md->mdhim_rank);
        return nullptr;
    }

    // copy the keys
    void **ks = new void *[num_keys]();
    int *k_lens = new int[num_keys]();
    if (!ks || !k_lens) {
        delete [] ks;
        delete [] k_lens;
        return nullptr;
    }

    for(int i = 0; i < num_keys; i++) {
        if (!(ks[i] = ::operator new(key_lens[i]))) {
            for(int j = 0; j < i; j++) {
                ::operator delete(ks[j]);
            }

            delete [] ks;
            delete [] k_lens;
            return nullptr;
        }
        memcpy(ks[i], keys[i], key_lens[i]);
        k_lens[i] = key_lens[i];
    }

    TransportBGetRecvMessage *bgrm_head = _bget_records(md, index, ks, k_lens, num_keys, 1, op);
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
                  md->mdhim_rank, MAX_BULK_OPS);
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
    if (!md || !md->p ||
        !key || !key_len) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    if (num_records > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Too many bulk operations requested in mdhimBGetOp",
              md->mdhim_rank);
        return nullptr;
    }

    if (op == TransportGetMessageOp::GET_EQ || op == TransportGetMessageOp::GET_PRIMARY_EQ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Invalid op specified for mdhimGet",
              md->mdhim_rank);
        return nullptr;
    }

    // copy the key
    void **k = new void *[1]();
    int *k_len = new int[1]();
    if (!k || !k_len) {
        delete [] k;
        delete [] k_len;
        return nullptr;
    }

    if (!(k[0] = ::operator new(key_len))) {
        delete [] k;
        delete [] k_len;
        return nullptr;
    }

    memcpy(k[0], key, key_len);
    k_len[0] = key_len;

    //Get the linked list of return messages from mdhimBGet
    return mdhim_bgrm_init(_bget_records(md, index, k, k_len, 1, num_records, op));
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
    if (!md || !md->p ||
        !key || !key_len) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    // Copy the key
    void **k = new void *[1]();
    int *k_len = new int[1]();
    if (!k || !k_len) {
        delete [] k;
        delete [] k_len;
        return nullptr;
    }

    if (!(k[0] = ::operator new(key_len))) {
        delete [] k;
        delete [] k_len;
        return nullptr;
    }

    memcpy(k[0], key, key_len);
    k_len[0] = key_len;

    return mdhim_brm_init(_bdel_records(md, index, k, k_len, 1));
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
    if (!md || !md->p ||
        !keys || !key_lens) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    // copy the keys
    void **ks = new void *[num_records]();
    int *k_lens = new int[num_records]();

    if (!ks || !k_lens) {
        delete [] ks;
        delete [] k_lens;
        return nullptr;
    }

    for(int i = 0; i < num_records; i++) {
        if (!(ks[i] = ::operator new(key_lens[i]))) {
            for(int j = 0; j < i; j++) {
                ::operator delete(ks[j]);
            }
            return nullptr;
        }
        memcpy(ks[i], keys[i], key_lens[i]);
        k_lens[i] = key_lens[i];
    }

    //Check to see that we were given a sane amount of records
    if (num_records > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Too many bulk operations requested in mdhimBDelete",
              md->mdhim_rank);
        return nullptr;
    }

    return mdhim_brm_init(_bdel_records(md, index, ks, k_lens, num_records));
}

/**
 * Retrieves statistics from all the range servers - collective call
 *
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimStatFlush(mdhim_t *md, index_t *index) {
    int ret;

    MPI_Barrier(md->mdhim_comm);
    if ((ret = get_stat_flush(md, index)) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Error while getting MDHIM stat data in mdhimStatFlush",
              md->mdhim_rank);
    }
    MPI_Barrier(md->mdhim_comm);

    return ret;
}

/**
 * Sets the secondary_info structure used in mdhimPut
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

    //Duplicate the input values
    sinfo->secondary_keys = new void *[num_keys]();
    sinfo->secondary_key_lens = new int[num_keys]();
    for(int i = 0; i < num_keys; i++) {
        sinfo->secondary_keys[i] = ::operator new(secondary_key_lens[i]);
        memcpy(sinfo->secondary_keys[i], secondary_keys[i], secondary_key_lens[i]);
        sinfo->secondary_key_lens[i] = secondary_key_lens[i];
    }

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
