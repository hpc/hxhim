#ifndef IS_RANGE_SERVER_H
#define IS_RANGE_SERVER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>

/** @description Utility function to determine whether or not a rank has/is a range server */
int is_range_server(const int rank, const size_t client_ratio, const size_t server_ratio);

#ifdef __cplusplus
}
#endif

#endif
