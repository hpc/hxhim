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
 * Usage:
 *
 *     hxhim_results_t *res = hxhimFlush(hx);
 *     for(hxhim_results_goto_head(res); hxhim_results_valid(res) == HXHIM_SUCCESS; hxhim_results_goto_next(res)) {
 *         hxhim_result_type_t type;
 *         hxhim_results_type(res, &type);
 *         switch (type) {
 *             case HXHIM_RESULT_PUT:
 *                 break;
 *             case HXHIM_RESULT_GET:
 *                 {
 *                     // do stuff with get
 *                 }
 *                 break;
 *             case HXHIM_RESULT_DEL:
 *                 break;
 *             default:
 *                 break;
 *         }
 *     }
 *     hxhim_results_destroy(res);
 *
 */

typedef enum hxhim_result_type {
    HXHIM_RESULT_NONE,
    HXHIM_RESULT_PUT,
    HXHIM_RESULT_GET,
    HXHIM_RESULT_DEL,
    HXHIM_RESULT_SYNC,
    HXHIM_RESULT_HISTOGRAM,
} hxhim_result_type_t;

// Opaque C struct, since user will never create one themselves
typedef struct hxhim_results hxhim_results_t;

// "iterator" functions
int hxhim_results_goto_head(hxhim_results_t *res);
int hxhim_results_goto_next(hxhim_results_t *res);

// accessor functions - only operates on current result in results list
int hxhim_results_valid(hxhim_results_t *res);                              /* whether or not the pointer is readable */
int hxhim_results_type(hxhim_results_t *res, enum hxhim_result_type *type);
int hxhim_results_error(hxhim_results_t *res, int *error);                  /* whether or not the results are good */
int hxhim_results_datastore(hxhim_results_t *res, int *datastore);
// these accessor functions only work for GET results
int hxhim_results_get_object_type(hxhim_results_t *res, enum hxhim_type_t *object_type);
int hxhim_results_get_subject(hxhim_results_t *res, void **subject, size_t *subject_len);
int hxhim_results_get_predicate(hxhim_results_t *res, void **predicate, size_t *predicate_len);
int hxhim_results_get_object(hxhim_results_t *res, void **object, size_t *object_len);

void hxhim_results_destroy(hxhim_t *hx, hxhim_results_t *res);

#ifdef __cplusplus
}
#endif

#endif
