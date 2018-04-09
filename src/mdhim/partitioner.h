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

#ifdef __cplusplus
extern "C"
{
#endif

//Used for hashing strings to the appropriate range server
struct mdhim_char {
    int id;            /* we'll use this field as the key */
    int pos;
    UT_hash_handle hh; /* makes this structure hashable */
};

typedef struct rangesrv_info rangesrv_info_t;
typedef struct rangesrv_list {
    struct rangesrv_info *ri;
    struct rangesrv_list *next;
} rangesrv_list_t;

void partitioner_init();
void partitioner_release();
rangesrv_list_t *get_range_servers(mdhim_t *md, index_t *index,
                                   void *key, int key_len);
rangesrv_info_t *get_range_server_by_slice(mdhim_t *md,
                                           index_t *index, int slice);
void build_alphabet();
int verify_key(index_t *index, void *key, int key_len, int key_type);
long double get_str_num(void *key, uint32_t key_len);
  //long double get_byte_num(void *key, uint32_t key_len);
uint64_t get_byte_num(void *key, uint32_t key_len);
int get_slice_num(mdhim_t *md, index_t *index, void *key, int key_len);
int is_float_key(int type);
rangesrv_list_t *get_range_servers_from_stats(mdhim_t *md, index_t *index,
                                              void *key, int key_len, TransportGetMessageOp op);

#ifdef __cplusplus
}
#endif
#endif
