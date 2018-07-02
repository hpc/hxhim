#ifndef      __LEVELDB_H
#define      __LEVELDB_H

#include <leveldb/c.h>

#include "data_store.h"

/* Function pointer for comparator in C */
typedef int (*mdhim_store_cmp_fn_t)(void* arg, const char* a, std::size_t alen,
                                    const char* b, std::size_t blen);

typedef struct mdhim_leveldb {
	leveldb_t *db;
	leveldb_options_t *options;
	leveldb_comparator_t* cmp;
	leveldb_filterpolicy_t *filter;
	leveldb_cache_t *cache;
	leveldb_env_t *env;
	leveldb_writeoptions_t *write_options;
	leveldb_readoptions_t *read_options;
	mdhim_store_cmp_fn_t compare;
} mdhim_leveldb_t;

int mdhim_leveldb_open(void **dbh, void **dbs, const char *path, int flags, int key_type, mdhim_db_options_t *opts);
int mdhim_leveldb_put(void *dbh, void *key, std::size_t key_len, void *data, std::size_t data_len);
int mdhim_leveldb_get(void *dbh, void *key, std::size_t key_len, void **data, std::size_t *data_len);
int mdhim_leveldb_get_next(void *dbh, void **key, std::size_t *key_len,
                           void **data, std::size_t *data_len);
int mdhim_leveldb_get_prev(void *dbh, void **key, std::size_t *key_len,
                           void **data, std::size_t *data_len);
int mdhim_leveldb_close(void *dbh, void *dbs);
int mdhim_leveldb_del(void *dbh, void *key, std::size_t key_len);
int mdhim_leveldb_commit(void *dbh);
int mdhim_leveldb_batch_put(void *dbh, void **key, std::size_t *key_lens,
                            void **data, std::size_t *data_lens, int num_records);
#endif
