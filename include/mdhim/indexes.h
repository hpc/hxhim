/*
 * MDHIM TNG
 *
 * Index abstraction
 */

#ifndef      __INDEX_H
#define      __INDEX_H

#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"
#include <mpi.h>

#include "index_struct.h"
#include "mdhim_struct.h"
#include "range_server_info_struct.h"

int update_stat(mdhim_t *md, index_t *bi, const int rs_id, void *key, std::size_t key_len);
int write_stat(mdhim_t *md, index_t *bi, const int rs_idx);
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

uint32_t get_num_range_servers(const int size, const int range_server_factor);
uint32_t get_num_databases(const int size, const int range_server_factor, const int dbs_per_server);

#endif
