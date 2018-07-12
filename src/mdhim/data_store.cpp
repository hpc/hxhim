/*
 * MDHIM TNG
 *
 * Data store abstraction
 */

#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include "mdhim/mdhim_options.h"
#include "mdhim/data_store.h"
#ifdef HXHIM_USE_LEVELDB
#include "mdhim/ds_leveldb.h"
#endif
#ifdef HXHIM_USE_ROCKSDB
#include "mdhim/ds_leveldb.h"
#endif
#ifdef SOPHIADB_SUPPORT
#include "mdhim/ds_sophia.h"
#endif
#ifdef HXHIM_USE_MYSQL
#include "mdhim/ds_mysql.h"
#endif

/**
 * mdhim_db_init
 * Initializes mdhim_store_t structure based on type
 *
 * @param type           in   Database store type to use (i.e., LEVELDB, etc)
 * @return mdhim_store_t      The mdhim storage abstraction struct
 */
mdhim_store_t *mdhim_db_init(int type) {
	mdhim_store_t *store;

	//Initialize the store structure
	store = (mdhim_store_t*)malloc(sizeof(mdhim_store_t));
	store->type = type;
	store->db_handle = NULL;
	store->db_stats = NULL;
	store->mdhim_store_stats = NULL;
    store->mdhim_store_stats_lock = PTHREAD_RWLOCK_INITIALIZER;

	switch(type) {

#ifdef HXHIM_USE_LEVELDB
	case LEVELDB:
		store->open = mdhim_leveldb_open;
		store->put = mdhim_leveldb_put;
		store->batch_put = mdhim_leveldb_batch_put;
		store->get = mdhim_leveldb_get;
		store->get_next = mdhim_leveldb_get_next;
		store->get_prev = mdhim_leveldb_get_prev;
		store->del = mdhim_leveldb_del;
		store->commit = mdhim_leveldb_commit;
		store->close = mdhim_leveldb_close;
		break;

#endif

// #ifdef HXHIM_USE_ROCKSDB
// 	case ROCKSDB:
// 		store->open = mdhim_leveldb_open;
// 		store->put = mdhim_leveldb_put;
// 		store->batch_put = mdhim_leveldb_batch_put;
// 		store->get = mdhim_leveldb_get;
// 		store->get_next = mdhim_leveldb_get_next;
// 		store->get_prev = mdhim_leveldb_get_prev;
// 		store->del = mdhim_leveldb_del;
// 		store->commit = mdhim_leveldb_commit;
// 		store->close = mdhim_leveldb_close;
// 		break;
// #endif

#ifdef HXHIM_USE_MYSQL
	case MYSQLDB:
		store->open = mdhim_mysql_open;
		store->put = mdhim_mysql_put;
		store->batch_put = mdhim_mysql_batch_put;
		store->get = mdhim_mysql_get;
		store->get_next = mdhim_mysql_get_next;
		store->get_prev = mdhim_mysql_get_prev;
		store->del = mdhim_mysql_del;
		store->commit = mdhim_mysql_commit;
		store->close = mdhim_mysql_close;
		break;
#endif

	default:
		free(store);
		store = NULL;
		break;
	}

	return store;
}
