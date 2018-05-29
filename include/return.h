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
int hxhim_return_move_to_first_spo(hxhim_return_t *ret);
int hxhim_return_prev_spo(hxhim_return_t *ret);
int hxhim_return_next_spo(hxhim_return_t *ret);
int hxhim_return_valid_spo(hxhim_return_t *ret, int *valid);
int hxhim_return_get_spo(hxhim_return_t *ret, void **subject, size_t *subject_len, void **predicate, size_t *predicate_len, void **object, size_t *object_len);
int hxhim_return_next(hxhim_return_t *ret);

#ifdef __cplusplus
}
#endif

#endif
