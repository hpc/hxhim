/*
 * MDHIM TNG
 *
 * Index abstraction
 */

#ifndef      __INDEX_H
#define      __INDEX_H

#include "mlog2.h"
#include "mlogfacs2.h"
#include <mpi.h>

#include "index_struct.h"
#include "mdhim_struct.h"
#include "range_server_info_struct.h"

int update_stat(mdhim_t *md, index_t *bi, const int rs_id, void *key, uint32_t key_len);
index_t *create_local_index(mdhim_t *md, int db_type, int key_type, const char *index_name);
index_t *create_global_index(mdhim_t *md, int server_factor,
				    uint64_t max_recs_per_slice, int db_type,
				    int key_type, char *index_name);
int get_rangesrvs(mdhim_t *md, index_t *index);
int32_t is_range_server(mdhim_t *md, int rank, index_t *index);
int index_init_comm(mdhim_t *md, index_t *bi);
int get_stat_flush(mdhim_t *md, index_t *index);
index_t *get_index(mdhim_t *md, int index_id);
index_t *get_index_by_name(mdhim_t *md, char *index_name);
void indexes_release(mdhim_t *md);
int im_range_server(index_t *index);

/** @description Internal function for converting a database id into a rank and index */
int _decompose_db(index_t *index, const int db, int *rank, int *rs_idx);

/** @description Internal function for converting a rank and index into a database id */
int _compose_db(index_t *index, int *db, const int rank, const int rs_idx);
int _compose_db(int *db, const int rank, const int dbs_per_server, const int range_server_factor, const int rs_idx);

uint32_t get_num_range_servers(const int size, const int range_server_factor);
uint32_t get_num_databases(const int size, const int range_server_factor, const int dbs_per_server);

#endif
