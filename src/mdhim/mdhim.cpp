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

#include "clone.hpp"
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
static int bootstrapInit(mdhim_t *md, mdhim_options_t *opts) {
    if (!md || !opts){
        return MDHIM_ERROR;
    }

    // do not allow MDHIM_COMM_NULL to be used
    if (opts->comm == MPI_COMM_NULL) {
        return MDHIM_ERROR;
    }

    md->comm = opts->comm;
    md->lock = PTHREAD_MUTEX_INITIALIZER;
    md->size = opts->size;
    md->rank = opts->rank;

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
    if (!md) {
        return MDHIM_ERROR;
    }

    memset(md, 0, sizeof(*md));

    if (!opts || !opts->p || !opts->p->transport || !opts->p->db) {
        return MDHIM_ERROR;
    }

    //Open mlog - stolen from plfs
    mlog_open((char *)"mdhim", 0, opts->p->db->debug_level, opts->p->db->debug_level, nullptr, 0, MLOG_LOGPID, 0);

    //Initialize bootstrapping variables
    if (bootstrapInit(md, opts) != MDHIM_SUCCESS){
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
        TransportBRecvMessage *brm = local_client_commit(md, static_cast<TransportMessage *>(cm));
        if (!brm || brm->error) {
            ret = MDHIM_ERROR;
            mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d mdhimCommit - "
                 "Error while committing to database",
                  md->rank);
        }

        delete brm;
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
mdhim_rm_t *mdhimPut(mdhim_t *md, index_t *index,
                      void *primary_key, std::size_t primary_key_len,
                      void *value, std::size_t value_len) {
    if (!md || !md->p ||
        !primary_key || !primary_key_len ||
        !value || !value_len) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    return mdhim_rm_init(_put_record(md, index, primary_key, primary_key_len, value, value_len));
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
                       void **primary_keys, std::size_t *primary_key_lens,
                       void **primary_values, std::size_t *primary_value_lens,
                       std::size_t num_records) {
    if (!md || !md->p ||
        !primary_keys || !primary_key_lens ||
        !primary_values || !primary_value_lens) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    return mdhim_brm_init(_bput_records(md, index,
                                        primary_keys, primary_key_lens,
                                        primary_values, primary_value_lens,
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
                         void *key, std::size_t key_len,
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

    return mdhim_grm_init(_get_record(md, index, key, key_len, op));
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
                          void **keys, std::size_t *key_lens,
                          std::size_t num_keys, enum TransportGetMessageOp op) {
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

    TransportBGetRecvMessage *bgrm_head = _bget_records(md, index, keys, key_lens, num_keys, 1, op);

    if (!bgrm_head) {
        return nullptr;
    }

    if (op == TransportGetMessageOp::GET_PRIMARY_EQ) {
        //Get the number of keys/values we received
        int plen = 0;
        while (bgrm_head) {
            for(std::size_t i = 0; i < bgrm_head->num_keys; i++) {
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
        std::size_t *primary_key_lens = new std::size_t[plen]();

        //Get the primary keys from the previously received messages' values
        plen = 0;
        while (bgrm_head) {
            for(std::size_t i = 0; i < bgrm_head->num_keys && plen < MAX_BULK_OPS; i++) {
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
                            void *key, std::size_t key_len,
                            std::size_t num_records, enum TransportGetMessageOp op) {
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

    //Get the linked list of return messages from mdhimBGet
    return mdhim_bgrm_init(_bget_records(md, index, &key, &key_len, 1, num_records, op));
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
                        void *key, std::size_t key_len) {
    if (!md || !md->p ||
        !key || !key_len) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    return mdhim_rm_init(_del_record(md, index, key, key_len));
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
                          void **keys, std::size_t *key_lens,
                          std::size_t num_records) {
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

    MPI_Barrier(md->comm);
    if ((ret = get_stat_flush(md, index)) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Error while getting MDHIM stat data in mdhimStatFlush",
              md->rank);
    }
    MPI_Barrier(md->comm);

    return ret;
}

/* what database would respond to this key? */
int mdhimWhichDB(mdhim_t *md, void *key, std::size_t key_len)
{
    return _which_db(md, key, key_len);
}

int mdhimDecomposeDB(mdhim_t *md, const int db, int *rank, int *rs_idx) {
    return _decompose_db(md->p->primary_index, db, rank, rs_idx);
}

int mdhimComposeDB(mdhim_t *md, int *db, const int rank, const int rs_idx) {
    return _compose_db(md->p->primary_index, db, rank, rs_idx);
}
