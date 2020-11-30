#ifndef HXHIM_RESULTS_H
#define HXHIM_RESULTS_H

#include <stddef.h>
#include <stdint.h>

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
 *     HXHIM_C_RESULTS_LOOP(res) {
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

/** Convenience macro to loop over a hxhim_results_t pointer */
#define HXHIM_C_RESULTS_LOOP(results)                               \
    for(hxhim_results_goto_head((results));                         \
        hxhim_results_valid_iterator((results)) == HXHIM_SUCCESS;   \
        hxhim_results_goto_next((results)))

int hxhim_results_valid(hxhim_results_t *res);                             /* whether or not the underlying pointer is readable */

// Accessors for the entire list of results
int hxhim_results_size(hxhim_results_t *res, size_t *size);
int hxhim_results_duration(hxhim_results_t *res, uint64_t *duration);

// "iterator" functions
int hxhim_results_valid_iterator(hxhim_results_t *res);                     /* whether or not the pointer is readable */
int hxhim_results_goto_head(hxhim_results_t *res);
int hxhim_results_goto_next(hxhim_results_t *res);

// accessor functions - only operates on current result in results list
int hxhim_result_op(hxhim_results_t *res, enum hxhim_op_t *op);
int hxhim_result_status(hxhim_results_t *res, int *status);                /* whether or not the results are good */
int hxhim_result_range_server(hxhim_results_t *res, int *range_server);
int hxhim_result_subject(hxhim_results_t *res, void **subject, size_t *subject_len, enum hxhim_data_t *subject_type);
int hxhim_result_predicate(hxhim_results_t *res, void **predicate, size_t *predicate_len, enum hxhim_data_t *predicate_type);
/* these accessor functions only work for GET results */
int hxhim_result_object(hxhim_results_t *res, void **object, size_t *object_len, enum hxhim_data_t *object_type);
/* this accessor function only works for HISTOGRAM results */
int hxhim_result_histogram(hxhim_results_t *res, const char **name, size_t *name_len, double **buckets, size_t **counts, size_t *size);

void hxhim_results_destroy(hxhim_results_t *res);

#ifdef __cplusplus
}
#endif

#endif
