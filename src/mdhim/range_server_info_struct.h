#ifndef RANGE_SERVER_INFO_H
#define RANGE_SERVER_INFO_H

#include <stdint.h>

#include "uthash.h"

/*
 * Range server info
 * Contains information about each range server
 */
typedef struct rangesrv_info {
	//The range server's rank in the comm
	uint32_t rank;
	//The range server's identifier based on rank and number of servers
	uint32_t rangesrv_num;
	UT_hash_handle hh;         /* makes this structure hashable */
} rangesrv_info_t;

#endif
