#ifndef HXHIM_RETURN_H
#define HXHIM_RETURN_H

#include <stdio.h>

#include "hxhim_work_op.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct hxhim_return hxhim_return_t;

void hxhim_return_destroy(hxhim_return_t *ret);

int hxhim_return_get_src(hxhim_return_t *ret, int *src);
int hxhim_return_get_op(hxhim_return_t *ret, enum hxhim_work_op *op);
int hxhim_return_get_error(hxhim_return_t *ret, int *error);
int hxhim_return_move_to_first_rs(hxhim_return_t *ret);
int hxhim_return_next_rs(hxhim_return_t *ret);
int hxhim_return_valid_rs(hxhim_return_t *ret, int *valid);
int hxhim_return_move_to_first_kv(hxhim_return_t *ret);
int hxhim_return_prev_kv(hxhim_return_t *ret);
int hxhim_return_next_kv(hxhim_return_t *ret);
int hxhim_return_valid_kv(hxhim_return_t *ret, int *valid);
int hxhim_return_get_kv(hxhim_return_t *ret, void **key, size_t *key_len, void **value, size_t *value_len);

#ifdef __cplusplus
}
#endif

#endif
