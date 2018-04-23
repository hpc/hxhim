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
#include "index_struct.h"
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
    if (!md){
        return MDHIM_ERROR;
    }

    md->comm = MPI_COMM_WORLD;
    md->lock = PTHREAD_MUTEX_INITIALIZER;

    //Get the size of the main MDHIM communicator
    if (MPI_Comm_size(md->comm, &md->size) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }

    //Get the rank of the main MDHIM communicator
    if (MPI_Comm_rank(md->comm, &md->rank) != MPI_SUCCESS) {
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

    md->comm = MPI_COMM_NULL;
    md->size = 0;
    md->rank = 0;

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

    //Initialize bootstrapping variables
    if (bootstrapInit(md) != MDHIM_SUCCESS){
        mlog(MDHIM_CLIENT_CRIT, "MDHIM - Error Bootstrap Initialization Failed");
        return MDHIM_ERROR;
    }

    //Initialize context variables based on options
    if (mdhim_private_init(md, opts->p->db, opts->p->transport) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d mdhimInit - Private Variable Initialization Failed", md->rank);
        return MDHIM_ERROR;
    }

    mlog(MDHIM_CLIENT_INFO, "MDHIM Rank %d mdhimInit - Completed Successfully", md->rank);
    return MDHIM_SUCCESS;
}

/**
 * Quits the MDHIM instance
 *
 * @param md MDHIM context to be closed
 * @return MDHIM status value
 */
int mdhimClose(mdhim_t *md) {
    mlog(MDHIM_CLIENT_INFO, "MDHIM Rank %d mdhimClose - Started", md->rank);

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
        cm->src = md->rank;
        cm->dst = md->rank;
        TransportRecvMessage *rm = local_client_commit(md, static_cast<TransportMessage *>(cm));
        if (!rm || rm->error) {
            ret = MDHIM_ERROR;
            mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d mdhimCommit - "
                 "Error while committing to database",
                  md->rank);
        }

        delete rm;
    }

    mlog(MDHIM_CLIENT_INFO, "MDHIM Rank %d mdhimCommit - Completed Successfully", md->rank);
    return ret;
}

/**
 * _clone
 * Allocate space for, and copy the contents of src into *dst
 *
 * @param src     the source address
 * @param src_len the length of the source
 * @param dst     address of the destination pointer
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int _clone(void *src, int src_len, void **dst) {
    if (!src || !src_len ||
        !dst) {
        return MDHIM_ERROR;
    }

    if (!(*dst = ::operator new(src_len))) {
        *dst = nullptr;
    }

    memcpy(*dst, src, src_len);
    return MDHIM_SUCCESS;
}

/**
 * _clone
 * Allocate space for, and copy the contents of srcs and src_lens into *dsts and *dst_lens
 *
 * @param srcs     the source addresses
 * @param src_lens the lengths of the source
 * @param dsts     addresses of the destination pointers
 * @param dst_lens address of the destination lengths
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int _clone(int count, void **srcs, int *src_lens, void ***dsts, int **dst_lens) {
    if (!count) {
        return MDHIM_SUCCESS;
    }

    if (!srcs || !src_lens ||
        !dsts || !dst_lens) {
        return MDHIM_ERROR;
    }

    *dsts = new void *[count]();
    *dst_lens = new int[count]();
    if (!*dsts || !*dst_lens) {
        delete [] *dsts;
        *dsts = nullptr;

        delete [] *dst_lens;
        *dst_lens = nullptr;

        return MDHIM_ERROR;
    }

    for(int i = 0; i < count; i++) {
        if (!((*dsts)[i] = ::operator new(src_lens[i]))) {
            for(int j = 0; j < i; j++) {
                ::operator delete((*dsts)[j]);
            }

            delete [] *dsts;
            *dsts = nullptr;

            delete [] *dst_lens;
            *dst_lens = nullptr;
        }

        memcpy((*dsts)[i], srcs[i], src_lens[i]);
        (*dst_lens)[i] = src_lens[i];
    }

    return MDHIM_SUCCESS;
}

/**
 * _cleanup
 * Deallocate arrays
 *
 * @param data address of the data
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int _cleanup(void *data) {
    ::operator delete(data);
    return MDHIM_SUCCESS;
}

/**
 * _cleanup
 * Deallocate arrays of arrays and their lengths
 *
 * @param count how many data-len pairs there are
 * @param data  address of the data
 * @param len   the lengths of the data
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int _cleanup(int count, void **data, int *len) {
    for(int i = 0; i < count; i++) {
        ::operator delete(data[i]);
    }

    delete [] data;
    delete [] len;
    return MDHIM_SUCCESS;
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
mdhim_rm_t *mdhimPut(mdhim_t *md, index_t *index,
                      void *primary_key, int primary_key_len,
                      void *value, int value_len) {
    mlog(MDHIM_CLIENT_INFO, "MDHIM Rank %d mdhimPut - Started", md->rank);

    if (!md || !md->p ||
        !primary_key || !primary_key_len ||
        !value || !value_len) {
        mlog(MDHIM_CLIENT_ERR, "MDHIM Rank %d mdhimPut - Bad Arguments", md->rank);
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    // Clone primary key and value
    void *pk = nullptr;
    void *val = nullptr;
    if ((_clone(primary_key, primary_key_len, &pk) != MDHIM_SUCCESS) ||
        (_clone(value, value_len, &val)            != MDHIM_SUCCESS)) {
        _cleanup(pk);
        _cleanup(val);
        mlog(MDHIM_CLIENT_ERR, "MDHIM Rank %d mdhimPut - Could not allocate memory", md->rank);
        return nullptr;
    }

    return mdhim_rm_init(_put_record(md, md->p->primary_index, pk, primary_key_len, val, value_len));
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
mdhim_brm_t *mdhimBPut(mdhim_t *md, index_t *index,
                       void **primary_keys, int *primary_key_lens,
                       void **primary_values, int *primary_value_lens,
                       int num_records) {
    if (!md || !md->p ||
        !primary_keys || !primary_key_lens ||
        !primary_values || !primary_value_lens) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    //Copy the keys and values
    void **pks = nullptr, **pvs = nullptr;
    int *pk_lens = nullptr, *pv_lens = nullptr;
    if ((_clone(num_records, primary_keys, primary_key_lens, &pks, &pk_lens)     != MDHIM_SUCCESS) ||
        (_clone(num_records, primary_values, primary_value_lens, &pvs, &pv_lens) != MDHIM_SUCCESS)) {
        _cleanup(num_records, pks, pk_lens);
        _cleanup(num_records, pvs, pv_lens);
        return nullptr;
    }

    TransportBRecvMessage *brm = _bput_records(md, md->p->primary_index,
                                               pks, pk_lens,
                                               pvs, pv_lens,
                                               num_records);

    // pk, pk_lens, pvs, pv_lens are created in _bput_records
    delete [] pks;
    delete [] pk_lens;
    delete [] pvs;
    delete [] pv_lens;

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
    void *k = nullptr;
    if (_clone(key, key_len, &k) != MDHIM_SUCCESS) {
        _cleanup(k);
        mlog(MDHIM_CLIENT_ERR, "MDHIM Rank %d mdhimGet - Could not allocate memory", md->rank);
        return nullptr;
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
              md->rank);
        return nullptr;
    }

    //Check to see that we were given a sane amount of records
    if (num_keys > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Too many bulk operations requested in mdhimBGet",
              md->rank);
        return nullptr;
    }

    // copy the keys
    void **ks = nullptr;
    int *k_lens = nullptr;
    if (_clone(num_keys, keys, key_lens, &ks, &k_lens) != MDHIM_SUCCESS) {
        _cleanup(num_keys, ks, k_lens);
        return nullptr;
    }

    TransportBGetRecvMessage *bgrm_head = _bget_records(md, index, ks, k_lens, num_keys, 1, op);

    // ks and k_lens are created in _bget_records
    delete [] ks;
    delete [] k_lens;

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
                  md->rank, MAX_BULK_OPS);
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
        _cleanup(plen, primary_keys, primary_key_lens);
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
              md->rank);
        return nullptr;
    }

    if (op == TransportGetMessageOp::GET_EQ || op == TransportGetMessageOp::GET_PRIMARY_EQ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Invalid op specified for mdhimGet",
              md->rank);
        return nullptr;
    }

    // copy the key
    void **k = nullptr;
    int *k_len = nullptr;
    if (_clone(1, &key, &key_len, &k, &k_len) != MDHIM_SUCCESS) {
        _cleanup(1, k, k_len);
        return nullptr;
    }

    //Get the linked list of return messages from mdhimBGet
    TransportBGetRecvMessage *bgrm = _bget_records(md, index, k, k_len, 1, num_records, op);

    // ks and k_lens are created in _bget_records
    delete [] k;
    delete [] k_len;

    return mdhim_bgrm_init(bgrm);
}

/**
 * Deletes a single record from MDHIM
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to delete
 * @param key_len   the length of the key
 * @return mdhim_rm_t * or nullptr on error
 */
mdhim_rm_t *mdhimDelete(mdhim_t *md, index_t *index,
                         void *key, int key_len) {
    if (!md || !md->p ||
        !key || !key_len) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    // Copy the key
    void *k = nullptr;
    if (_clone(key, key_len, &k) != MDHIM_SUCCESS) {
        _cleanup(k);
        return nullptr;
    }

    return mdhim_rm_init(_del_record(md, index, k, key_len));
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

    //Check to see that we were given a sane amount of records
    if (num_records > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Too many bulk operations requested in mdhimBDelete",
              md->rank);
        return nullptr;
    }

    // copy the keys
    void **ks = nullptr;
    int *k_lens = nullptr;
    if (_clone(num_records, keys, key_lens, &ks, &k_lens) != MDHIM_SUCCESS) {
        _cleanup(1, ks, k_lens);
        return nullptr;
    }

    TransportBRecvMessage *brm = _bdel_records(md, index, ks, k_lens, num_records);

    // ks and k_lens are created in _bget_records
    delete [] ks;
    delete [] k_lens;

    return mdhim_brm_init(brm);
}

/**
 * Retrieves statistics from all the range servers - collective call
 *
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimStatFlush(mdhim_t *md, index_t *index) {
    int ret;

    MPI_Barrier(md->comm);
    if ((ret = get_stat_flush(md, index)) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Error while getting MDHIM stat data in mdhimStatFlush",
              md->rank);
    }
    MPI_Barrier(md->comm);

    return ret;
}

/* what server would respond to this key? */
int mdhimWhichServer(mdhim_t *md, void *key, int key_len)
{
    return(_which_server(md, key, key_len));
}
