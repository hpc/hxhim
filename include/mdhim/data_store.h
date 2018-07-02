/*
 * MDHIM TNG
 *
 * Data store abstraction
 */

#ifndef      __STORE_H
#define      __STORE_H

#include <pthread.h>
#include "utils/uthash.h"

#include "mdhim_options_private.h"
#include "mdhim/constants.h"

/* Function pointers for abstracting data stores */
typedef int (*mdhim_store_open_fn_t)(void **db_handle, void **db_stats, const char *path, int flags,
                                     int key_type, mdhim_db_options_t *opts);
typedef int (*mdhim_store_put_fn_t)(void *db_handle, void *key, std::size_t key_len,
                                    void *data, std::size_t data_len);
typedef int (*mdhim_store_batch_put_fn_t)(void *db_handle, void **keys, std::size_t *key_lens,
                                          void **data, std::size_t *data_lens, int num_records);
typedef int (*mdhim_store_get_fn_t)(void *db_handle, void *key, std::size_t key_len, void **data, std::size_t *data_len);
typedef int (*mdhim_store_get_next_fn_t)(void *db_handle, void **key,
                                         std::size_t *key_len, void **data,
                                         std::size_t *data_len);
typedef int (*mdhim_store_get_prev_fn_t)(void *db_handle, void **key,
                                         std::size_t *key_len, void **data,
                                         std::size_t *data_len);
typedef int (*mdhim_store_del_fn_t)(void *db_handle, void *key, std::size_t key_len);
typedef int (*mdhim_store_commit_fn_t)(void *db_handle);
typedef int (*mdhim_store_close_fn_t)(void *db_handle, void *db_stats);

//Used for storing stats in a hash table
typedef struct mdhim_stat {
    int key;                   //Key (slice number)
    void *max;                 //Max key
    void *min;                 //Min key
    int dirty;                 //Wether this stat was updated or a new stat
    uint64_t num;              //Number of keys in this slice
    struct mdhim_stat *stats;  //Used for local index stats to create a multi-level hash table
    UT_hash_handle hh;         /* makes this structure hashable */
} mdhim_stat_t;

//Used for storing stats in the database
typedef struct mdhim_db_stat {
    std::size_t slice;
    uint64_t imax;
    uint64_t imin;
    long double dmax;
    long double dmin;
    uint64_t num;
} mdhim_db_stat_t;

//Used for transmitting integer stats to all nodes
typedef struct mdhim_db_istat {
    std::size_t slice;
    uint64_t num;
    uint64_t imax;
    uint64_t imin;
} mdhim_db_istat_t;

//Used for transmitting float stats to all nodes
typedef struct mdhim_db_fstat {
    std::size_t slice;
    uint64_t num;
    long double dmax;
    long double dmin;
} mdhim_db_fstat_t;

/* Generic mdhim storage object */
typedef struct mdhim_store {
    int type;
    //handle to db
    void *db_handle;
    //Handle to db for stats
    void *db_stats;
    //Pointers to functions based on data store
    mdhim_store_open_fn_t open;
    mdhim_store_put_fn_t put;
    mdhim_store_batch_put_fn_t batch_put;
    mdhim_store_get_fn_t get;
    mdhim_store_get_next_fn_t get_next;
    mdhim_store_get_prev_fn_t get_prev;
    mdhim_store_del_fn_t del;
    mdhim_store_commit_fn_t commit;
    mdhim_store_close_fn_t close;

    //Login credentials
    char *db_user;
    char *db_upswd;
    char *dbs_user;
    char *dbs_upswd;
    char *db_host;
    char *dbs_host;

    //Hashtable for stats
    mdhim_stat_t *mdhim_store_stats;

    //Lock to allow concurrent readers and a single writer to the mdhim_store_stats
    pthread_rwlock_t mdhim_store_stats_lock;
} mdhim_store_t;

//Initializes the data store based on the type given (i.e., LEVELDB, etc...)
mdhim_store_t *mdhim_db_init(int db_type);
#endif
