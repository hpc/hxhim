#ifndef      __HASH_H
#define      __HASH_H

#include "uthash.h"

#include "indexes.h"
#include "mdhim_constants.h"
#include "mdhim_struct.h"
#include "transport.hpp"

/* Used to determine if a rank is a range server
   Works like this:
   if myrank % RANGE_SERVER_FACTOR == 0, then I'm a range server
   if all the keys haven't been covered yet

   if the number of ranks is less than the RANGE_SERVER_FACTOR,
   then the last rank will be the range server
*/

//Used for hashing strings to the appropriate range server
typedef struct mdhim_char {
    int id;            /* we'll use this field as the key */
    int pos;
    UT_hash_handle hh; /* makes this structure hashable */
} mdhim_char_t;

typedef struct rangesrv_info rangesrv_info_t;
typedef struct rangesrv_list {
    struct rangesrv_info *ri;
    struct rangesrv_list *next;
} rangesrv_list_t;

void partitioner_init();
void partitioner_release();
void _add_to_rangesrv_list(rangesrv_list_t **list, rangesrv_info_t *ri);

rangesrv_list_t *get_range_servers(const int size, index_t *index,
                                   void *key, int key_len);

rangesrv_info_t *get_range_server_by_slice(index_t *index, const int slice);

void build_alphabet();
long double get_str_num(void *key, uint32_t key_len);
uint64_t get_byte_num(void *key, uint32_t key_len);
int get_slice_num(const int key_type, uint64_t slice_size, void *key, int key_len);
int is_float_key(int type);
rangesrv_list_t *get_range_servers_from_stats(const int rank, index_t *index,
                                              void *key, int key_len, TransportGetMessageOp op);

#endif
