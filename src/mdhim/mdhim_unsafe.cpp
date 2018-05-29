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
 * Put
 * Inserts a single record into MDHIM into the given database
 *
 * @param md main MDHIM context
 * @param primary_key        pointer to key to store
 * @param primary_key_len    the length of the key
 * @param value              pointer to the value to store
 * @param value_len          the length of the value
 * @param database           the database to put the key value pair into
 * @param secondary_info     secondary global and local information for
 inserting secondary global and local keys
 * @return TransportRecvMessage * or nullptr on error
 */
TransportRecvMessage *mdhim::Unsafe::Put(mdhim_t *md, index_t *index,
                                         void *primary_key, std::size_t primary_key_len,
                                         void *value, std::size_t value_len,
                                         const int database) {
    if (!md || !md->p ||
        !primary_key || !primary_key_len ||
        !value || !value_len) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    return _put_record(md, index, primary_key, primary_key_len, value, value_len, database);
}

/**
 * mdhimUnsafePut
 * Inserts a single record into MDHIM into the given database
 *
 * @param md main MDHIM context
 * @param primary_key        pointer to key to store
 * @param primary_key_len    the length of the key
 * @param value              pointer to the value to store
 * @param value_len          the length of the value
 * @param database           the database to put the key value pair into
 * @return mdhim_brm_t * or nullptr on error
 */
mdhim_rm_t *mdhimUnsafePut(mdhim_t *md, index_t *index,
                           void *primary_key, std::size_t primary_key_len,
                           void *value, std::size_t value_len,
                           const int database) {
    return mdhim_rm_init(mdhim::Unsafe::Put(md, index, primary_key, primary_key_len, value, value_len, database));
}

/**
 * BPut
 * Inserts multiple records into MDHIM into the given databases
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to store
 * @param key_lens     array with lengths of each key in keys
 * @param values       pointer to array of values to store
 * @param value_lens   array with lengths of each value
 * @param databases    the databases to put the key value pairs into
 * @param num_records  the number of records to store (i.e., the number of keys in keys array)
 * @return TransportBRecvMessage * or nullptr on error
 */
TransportBRecvMessage *mdhim::Unsafe::BPut(mdhim_t *md, index_t *index,
                                           void **primary_keys, std::size_t *primary_key_lens,
                                           void **primary_values, std::size_t *primary_value_lens,
                                           const int *databases,
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
                         databases,
                         num_records);
}

/**
 * mdhimUnsafeBPut
 * Inserts multiple records into MDHIM into the given database
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to store
 * @param key_lens     array with lengths of each key in keys
 * @param values       pointer to array of values to store
 * @param value_lens   array with lengths of each value
 * @param databases    the databases to put the key value pairs into
 * @param num_records  the number of records to store (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or nullptr on error
 */
mdhim_brm_t *mdhimUnsafeBPut(mdhim_t *md, index_t *index,
                             void **primary_keys, std::size_t *primary_key_lens,
                             void **primary_values, std::size_t *primary_value_lens,
                             const int *databases,
                             std::size_t num_records) {
    return mdhim_brm_init(mdhim::Unsafe::BPut(md, index,
                                              primary_keys, primary_key_lens,
                                              primary_values, primary_value_lens,
                                              databases,
                                              num_records));
}

/**
 * Get
 * Retrieves a single record from MDHIM from the given database
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to get value of or last key to start from if op is
 (MDHIM_GET_NEXT or MDHIM_GET_PREV)
 * @param key_len   the length of the key
 * @param database  the database to get the key value pair from
 * @param op        the operation type
 * @return TransportGetRecvMessage * or nullptr on error
 */
TransportGetRecvMessage *mdhim::Unsafe::Get(mdhim_t *md, index_t *index,
                                            void *key, std::size_t key_len,
                                            const int database,
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

    return _get_record(md, index, key, key_len, database, op);
}

/**
 * mdhimUnsafeGet
 * Retrieves a single record from MDHIM from the given database
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to get value of or last key to start from if op is
 (MDHIM_GET_NEXT or MDHIM_GET_PREV)
 * @param key_len   the length of the key
 * @param database  the database to get the key value pair from
 * @param op        the operation type
 * @return mdhim_grm_t * or nullptr on error
 */
mdhim_grm_t *mdhimUnsafeGet(mdhim_t *md, index_t *index,
                            void *key, std::size_t key_len,
                            const int database,
                            enum TransportGetMessageOp op) {
    return mdhim_grm_init(mdhim::Unsafe::Get(md, index, key, key_len, database, op));
}

/**
 * BGet
 * Retrieves multiple records from MDHIM from the given databases
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to get values for
 * @param key_lens     array with lengths of each key in keys
 * @param databases    the databases to get the key value pairs from
 * @param num_records  the number of keys to get (i.e., the number of keys in keys array)
 * @return TransportBGetRecvMessage * or nullptr on error
 */
TransportBGetRecvMessage *mdhim::Unsafe::BGet(mdhim_t *md, index_t *index,
                                              void **keys, std::size_t *key_lens,
                                              const int *databases,
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

    TransportBGetRecvMessage *bgrm_head = _bget_records(md, index, keys, key_lens, databases, num_keys, 1, op);
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
                                  databases,
                                  plen, 1, TransportGetMessageOp::GET_EQ);

        //Free up the primary keys and lens arrays
        _cleanup(plen, primary_keys, primary_key_lens);
    }

    //Return the head of the list
    return bgrm_head;
}

/**
 * mdhimUnsafeBGet
 * Retrieves multiple records from MDHIM from the given databases
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to get values for
 * @param key_lens     array with lengths of each key in keys
 * @param database     the database to delete the key value pair from
 * @param num_records  the number of keys to get (i.e., the number of keys in keys array)
 * @return mdhim_bgrm_t * or nullptr on error
 */
mdhim_bgrm_t *mdhimUnsafeBGet(mdhim_t *md, index_t *index,
                              void **keys, std::size_t *key_lens,
                              const int *databases,
                              std::size_t num_keys, enum TransportGetMessageOp op) {
    return mdhim_bgrm_init(mdhim::Unsafe::BGet(md, index,
                                               keys, key_lens,
                                               databases,
                                               num_keys, op));
}

/**
 * BGetOp
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
 * @return TransportBGetRecvMessage * or nullptr on error
 */
TransportBGetRecvMessage *mdhim::Unsafe::BGetOp(mdhim_t *md, index_t *index,
                                                void *key, std::size_t key_len,
                                                const int database,
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
    return _bget_records(md, index, &key, &key_len, &database, 1, num_records, op);
}

/**
 * mdhimUnsafeBGetOp
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
 * @return mdhim_bgrm_t * or nullptr on error
 */
mdhim_bgrm_t *mdhimUnsafeBGetOp(mdhim_t *md, index_t *index,
                                void *key, std::size_t key_len,
                                const int database,
                                std::size_t num_records, enum TransportGetMessageOp op) {
    return mdhim_bgrm_init(mdhim::Unsafe::BGetOp(md, index, key, key_len, database, num_records, op));
}

/**
 * Delete
 * Deletes a single record from MDHIM from the given database
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to delete
 * @param key_len   the length of the key
 * @param database  the database to delete the key value pair from
 * @return TransportRecvMessage * or nullptr on error
 */
TransportRecvMessage *mdhim::Unsafe::Delete(mdhim_t *md, index_t *index,
                                            void *key, std::size_t key_len,
                                            const int database) {
    if (!md || !md->p ||
        !key || !key_len) {
        return nullptr;
    }

    if (!index) {
        index = md->p->primary_index;
    }

    return _del_record(md, index, key, key_len, database);
}

/**
 * mdhimUnsafeDelete
 * Deletes a single record from MDHIM from the given database
 *
 * @param md main MDHIM struct
 * @param key       pointer to key to delete
 * @param key_len   the length of the key
 * @param database  the database to delete the key from
 * @return mdhim_rm_t * or nullptr on error
 */
mdhim_rm_t *mdhimUnsafeDelete(mdhim_t *md, index_t *index,
                              void *key, std::size_t key_len,
                              const int database) {
    return mdhim_rm_init(mdhim::Unsafe::Delete(md, index, key, key_len, database));
}

/**
 * BDelete
 * Deletes multiple records from MDHIM from the given databases
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to delete
 * @param key_lens     array with lengths of each key in keys
 * @param databases    the databases to delete the key from
 * @param num_records  the number of keys to delete (i.e., the number of keys in keys array)
 * @return TransportBRecvMessage * or nullptr on error
 */
TransportBRecvMessage *mdhim::Unsafe::BDelete(mdhim_t *md, index_t *index,
                                              void **keys, std::size_t *key_lens,
                                              const int *databases,
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

    return _bdel_records(md, index, keys, key_lens, databases, num_records);
}

/**
 * mdhimUnsafeBDelete
 * Deletes multiple records from MDHIM from the given databases
 *
 * @param md main MDHIM struct
 * @param keys         pointer to array of keys to delete
 * @param key_lens     array with lengths of each key in keys
 * @param databases    the databases to delete the key from
 * @param num_records  the number of keys to delete (i.e., the number of keys in keys array)
 * @return mdhim_brm_t * or nullptr on error
 */
mdhim_brm_t *mdhimBDeleteDB(mdhim_t *md, index_t *index,
                            void **keys, std::size_t *key_lens,
                            const int *databases,
                            std::size_t num_records) {
    return mdhim_brm_init(mdhim::Unsafe::BDelete(md, index, keys, key_lens, databases, num_records));
}
