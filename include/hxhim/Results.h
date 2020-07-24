#ifndef HXHIM_RESULTS_H
#define HXHIM_RESULTS_H

#include <stddef.h>

#include "hxhim/constants.h"
#include "hxhim/struct.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Results
 * This structure holds a linked list of all results waiting to be used by a user.
 * A single result node contains exactly 1 set of data. Results of bulk operations
 * are flattened when they are stored.
 *
 * Each result takes ownership of the pointers passed into the constructor.
 *
 * To access the data, iterate through each result and use the hxhim_results_* functions.
 *
 * Usage:
 *
 *     hxhim_results_t *res = hxhimFlush(hx);
 *     for(hxhim_results_goto_head(res); hxhim_results_valid_iterator(res) == HXHIM_SUCCESS; hxhim_results_goto_next(res)) {
 *         enum hxhim_op_t op;
 *         hxhim_result_op(res, &op);
 *         switch (op) {
 *             case HXHIM_PUT:
 *                 // do stuff with put
 *                 break;
 *             case HXHIM_GET:
 *             case HXHIM_GETOP:
 *                 // do stuff with get
 *                 break;
 *             case HXHIM_DELETE:
 *                 // do stuff with del
 *                 break;
 *             case HXHIM_HISTOGRAM:
 *                 // do stuff with hist
 *                 break;
 *             default:
 *                 break;
 *         }
 *     }
 *     hxhim_results_destroy(res);
 *
 */

// Opaque C struct, since user will never create one themselves
typedef struct hxhim_results hxhim_results_t;

int hxhim_results_valid(hxhim_results_t *res);                             /* whether or not the underlying pointer is readable */

// Accessors for the entire list of results
int hxhim_results_size(hxhim_results_t *res, size_t *size);
int hxhim_results_duration(hxhim_results_t *res, long double *duration);

// "iterator" functions
int hxhim_results_valid_iterator(hxhim_results_t *res);                     /* whether or not the pointer is readable */
int hxhim_results_goto_head(hxhim_results_t *res);
int hxhim_results_goto_next(hxhim_results_t *res);

// accessor functions - only operates on current result in results list
int hxhim_result_op(hxhim_results_t *res, enum hxhim_op_t *op);
int hxhim_result_status(hxhim_results_t *res, int *status);                /* whether or not the results are good */
int hxhim_result_datastore(hxhim_results_t *res, int *datastore);
int hxhim_result_subject(hxhim_results_t *res, void **subject, size_t *subject_len);
int hxhim_result_predicate(hxhim_results_t *res, void **predicate, size_t *predicate_len);
/* these accessor functions only work for GET results */
int hxhim_result_object_type(hxhim_results_t *res, enum hxhim_object_type_t *object_type);
int hxhim_result_object(hxhim_results_t *res, void **object, size_t *object_len);
/* this accessor function only works for HISTOGRAM results */
int hxhim_result_histogram(hxhim_results_t *res, double **buckets, size_t **counts, size_t *size);

void hxhim_results_destroy(hxhim_results_t *res);

#ifdef __cplusplus
}
#endif

#endif
