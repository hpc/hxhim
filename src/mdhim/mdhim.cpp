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

#include "mlog2.h"
#include "mlogfacs2.h"

#include "clone.hpp"
#include "data_store.h"
#include "index_struct.h"
#include "local_client.h"
#include "mdhim.h"
#include "mdhim.hpp"
#include "mdhim_options.h"
#include "mdhim_options_private.h"
#include "mdhim_private.h"
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
    if ((md->comm = opts->comm) == MPI_COMM_NULL) {
        return MDHIM_ERROR;
    }

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
    if (!md){
        return MDHIM_ERROR;
    }

    md->comm = MPI_COMM_NULL;
    md->size = 0;
    md->rank = 0;

    return MDHIM_SUCCESS;
}

/**
 * Init
 * Initializes MDHIM
 *
 * @param md MDHIM context
 * @param opts Options structure for DB creation, such as name, and primary key type
 * @return MDHIM status value
 */
int mdhim::Init(mdhim_t *md, mdhim_options_t *opts) {
    if (!md) {
        return MDHIM_ERROR;
    }

    memset(md, 0, sizeof(*md));

    if (!opts) {
        return MDHIM_ERROR;
    }

    //Open mlog - stolen from plfs
    mlog_open((char *) "mdhim", 0, opts->debug_level, opts->debug_level, nullptr, 0, MLOG_LOGPID, 0);

    //Initialize bootstrapping variables
    if (bootstrapInit(md, opts) != MDHIM_SUCCESS){
        mlog(MDHIM_CLIENT_CRIT, "MDHIM - Error Bootstrap Initialization Failed");
        return MDHIM_ERROR;
    }

    if (!opts->p) {
        return MDHIM_ERROR;
    }

    //Initialize context variables based on options
    if (mdhim_private_init(md, opts->p->db, opts->p->transport) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d mdhimInit - Private Variable Initialization Failed", md->rank);
        return MDHIM_ERROR;
    }

    mlog(MDHIM_CLIENT_INFO, "MDHIM Rank %d mdhimInit - Completed Successfully", md->rank);
    MPI_Barrier(md->comm);

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
int mdhimInit(mdhim_t *md, mdhim_options_t *opts) {
    return mdhim::Init(md, opts);
}

/**
 * Close
 * Quits the MDHIM instance
 *
 * @param md MDHIM context to be closed
 * @return MDHIM_SUCCESS - all errors are ignored
 */
int mdhim::Close(mdhim_t *md) {
    if (md) {
        if (md->comm != MPI_COMM_NULL) {
            MPI_Barrier(md->comm);
        }

        mlog(MDHIM_CLIENT_INFO, "MDHIM Rank %d mdhimClose - Started", md->rank);

        mdhim_private_destroy(md);

        //Clean up bootstrapping variables
        bootstrapDestroy(md);

        //Close MLog
        mlog_close();
    }

    return MDHIM_SUCCESS;
}

/**
 * mdhimClose
 * Quits the MDHIM instance
 *
 * @param md MDHIM context to be closed
 * @return MDHIM status value
 */
int mdhimClose(mdhim_t *md) {
    return mdhim::Close(md);
}

/**
 * Commit
 * Commits outstanding MDHIM writes
 *
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhim::Commit(mdhim_t *md, index_t *index) {
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
 * mdhimCommit
 * Commits outstanding MDHIM writes
 *
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimCommit(mdhim_t *md, index_t *index) {
    return mdhim::Commit(md, index);
}

/**
 * Put
 * Inserts a single record into MDHIM
 *
 * @param md main MDHIM context
 * @param primary_key        pointer to key to store
 * @param primary_key_len    the length of the key
 * @param value              pointer to the value to store
 * @param value_len          the length of the value
 * @param secondary_info     secondary global and local information for
 inserting secondary global and local keys
 * @return TransportRecvMessage * or nullptr on error
 */
TransportRecvMessage *mdhim::Put(mdhim_t *md, index_t *index,
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

    return _put_record(md, index, primary_key, primary_key_len, value, value_len);
}

/**
 * mdhimPut
 * Inserts a single record into MDHIM
 *
 * @param md main MDHIM context
 * @param primary_key        pointer to key to store
 * @param primary_key_len    the length of the key
 * @param value              pointer to the value to store
 * @param value_len          the length of the value
 * @param secondary_info     secondary global and local information for
 inserting secondary global and local keys
 * @return mdhim_brm_t * or nullptr on error
 */
mdhim_rm_t *mdhimPut(mdhim_t *md, index_t *index,
                     void *primary_key, std::size_t primary_key_len,
                     void *value, std::size_t value_len) {
    return mdhim_rm_init(mdhim::Put(md, index, primary_key, primary_key_len, value, value_len));
}

/**
 * BPut
 * Inserts multiple records into MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to store
 * @param key_lens     array with lengths of each key in keys
 * @param values       pointer to array of values to store
 * @param value_lens   array with lengths of each value
 * @param num_records  the number of records to store (i.e., the number of keys in keys array)
 * @return TransportBRecvMessage * or nullptr on error
 */
TransportBRecvMessage *mdhim::BPut(mdhim_t *md, index_t *index,
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

    if (num_records > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Too many bulk operations requested in mdhimBPut",
             md->rank);
        return nullptr;
    }

    return _bput_records(md, index,
                         primary_keys, primary_key_lens,
                         primary_values, primary_value_lens,
                         num_records);
}

/**
 * mdhimBPut
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
    return mdhim_brm_init(mdhim::BPut(md, index,
                                      primary_keys, primary_key_lens,
                                      primary_values, primary_value_lens,
                                      num_records));
}

/**
 * Get
 * Retrieves a single record from MDHIM
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to get value of or last key to start from if op is
 (MDHIM_GET_NEXT or MDHIM_GET_PREV)
 * @param key_len   the length of the key
 * @param op        the operation type
 * @return TransportGetRecvMessage * or nullptr on error
 */
TransportGetRecvMessage *mdhim::Get(mdhim_t *md, index_t *index,
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

    return _get_record(md, index, key, key_len, op);
}

/**
 * mdhimGet
 * Retrieves a single record from MDHIM
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to get value of or last key to start from if op is
 (MDHIM_GET_NEXT or MDHIM_GET_PREV)
 * @param key_len   the length of the key
 * @param op        the operation type
 * @return mdhim_grm_t * or nullptr on error
 */
mdhim_grm_t *mdhimGet(mdhim_t *md, index_t *index,
                      void *key, std::size_t key_len,
                      enum TransportGetMessageOp op) {
    return mdhim_grm_init(mdhim::Get(md, index, key, key_len, op));
}

/**
 * BGet
 * Retrieves multiple records from MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to get values for
 * @param key_lens     array with lengths of each key in keys
 * @param num_records  the number of keys to get (i.e., the number of keys in keys array)
 * @return TransportBGetRecvMessage * or nullptr on error
 */
TransportBGetRecvMessage *mdhim::BGet(mdhim_t *md, index_t *index,
                                      void **keys, std::size_t *key_lens,
                                      std::size_t num_keys, enum TransportGetMessageOp op) {
    if (!md || !md->p ||
        !keys || !key_lens) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    if (num_keys > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Too many bulk operations requested in mdhimBPut",
             md->rank);
        return nullptr;
    }

    if (op != TransportGetMessageOp::GET_EQ && op != TransportGetMessageOp::GET_PRIMARY_EQ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Invalid operation for mdhimBGet",
             md->rank);
        return nullptr;
    }

    TransportBGetRecvMessage *bgrm_head = _bget_records(md, index, keys, key_lens, num_keys, 1, op);
    if (!bgrm_head) {
        return nullptr;
    }

    if (op == TransportGetMessageOp::GET_PRIMARY_EQ) {
        //Get the number of keys/values we received
        TransportBGetRecvMessage *bgrm = bgrm_head;
        int plen = 0;
        while (bgrm) {
            plen += bgrm->num_keys;
            bgrm = bgrm->next;
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
        while (bgrm_head) {
            for(std::size_t i = 0; i < bgrm_head->num_keys && i < MAX_BULK_OPS; i++) {
                primary_keys[i] = ::operator new(bgrm_head->value_lens[i]);
                memcpy(primary_keys[i], bgrm_head->values[i], bgrm_head->value_lens[i]);
                primary_key_lens[i] = bgrm_head->value_lens[i];
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
    return bgrm_head;
}

/**
 * mdhimBGet
 * Retrieves multiple records from MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to get values for
 * @param key_lens     array with lengths of each key in keys
 * @param num_records  the number of keys to get (i.e., the number of keys in keys array)
 * @return mdhim_bgrm_t * or nullptr on error
 */
mdhim_bgrm_t *mdhimBGet(mdhim_t *md, index_t *index,
                        void **keys, std::size_t *key_lens,
                        std::size_t num_keys, enum TransportGetMessageOp op) {
    return mdhim_bgrm_init(mdhim::BGet(md, index,
                                       keys, key_lens,
                                       num_keys, op));
}

/**
 * Delete
 * Deletes a single record from MDHIM
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to delete
 * @param key_len   the length of the key
 * @return TransportRecvMessage * or nullptr on error
 */
TransportRecvMessage *mdhim::Delete(mdhim_t *md, index_t *index,
                                    void *key, std::size_t key_len) {
    if (!md || !md->p ||
        !key || !key_len) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    return _del_record(md, index, key, key_len);
}

/**
 * mdhimDelete
 * Deletes a single record from MDHIM
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to delete
 * @param key_len   the length of the key
 * @return mdhim_rm_t * or nullptr on error
 */
mdhim_rm_t *mdhimDelete(mdhim_t *md, index_t *index,
                        void *key, std::size_t key_len) {
    return mdhim_rm_init(mdhim::Delete(md, index, key, key_len));
}

/**
 * BDelete
 * Deletes multiple records from MDHIM
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to delete
 * @param key_lens     array with lengths of each key in keys
 * @param num_records  the number of keys to delete (i.e., the number of keys in keys array)
 * @return TransportBRecvMessage * or nullptr on error
 */
TransportBRecvMessage *mdhim::BDelete(mdhim_t *md, index_t *index,
                                      void **keys, std::size_t *key_lens,
                                      std::size_t num_records) {
    if (!md || !md->p ||
        !keys || !key_lens) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    if (num_records > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "Too many bulk operations requested in mdhimBDelete",
             md->rank);
        return nullptr;
    }

    return _bdel_records(md, index, keys, key_lens, num_records);
}

/**
 * mdhimBDelete
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
    return mdhim_brm_init(mdhim::BDelete(md, index, keys, key_lens, num_records));
}

/**
 * StatFlush
 * Retrieves statistics from all the range servers - collective call
 *
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhim::StatFlush(mdhim_t *md, index_t *index) {
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

/**
 * mdhimStatFlush
 * Retrieves statistics from all the range servers - collective call
 *
 * @param md main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int mdhimStatFlush(mdhim_t *md, index_t *index) {
    return mdhim::StatFlush(md, index);
}

int mdhim::DBCount(mdhim_t *md) {
    if (!md || !md->p || !md->p->primary_index) {
        return MDHIM_ERROR;
    }

    return get_num_databases(md->size, md->p->primary_index->range_server_factor, md->p->primary_index->dbs_per_server);
}

int mdhimDBCount(mdhim_t *md) {
    return mdhim::DBCount(md);
}

int mdhim::WhichDB(mdhim_t *md, void *key, std::size_t key_len)
{
    return _which_db(md, key, key_len);
}

int mdhimWhichDB(mdhim_t *md, void *key, std::size_t key_len)
{
    return mdhim::WhichDB(md, key, key_len);
}

int mdhim::DecomposeDB(mdhim_t *md, const int db, int *rank, int *rs_idx) {
    return _decompose_db(md->p->primary_index, db, rank, rs_idx);
}

int mdhimDecomposeDB(mdhim_t *md, const int db, int *rank, int *rs_idx) {
    return mdhim::DecomposeDB(md, db, rank, rs_idx);
}

int mdhim::ComposeDB(mdhim_t *md, int *db, const int rank, const int rs_idx) {
    return _compose_db(md->p->primary_index, db, rank, rs_idx);
}

int mdhimComposeDB(mdhim_t *md, int *db, const int rank, const int rs_idx) {
    return mdhim::ComposeDB(md, db, rank, rs_idx);
}
