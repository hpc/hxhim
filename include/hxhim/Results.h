#ifndef HXHIM_RESULTS_H
#define HXHIM_RESULTS_H

#include <stddef.h>

#include "hxhim/Stats.h"
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
 *         hxhim_result_type_t type;
 *         hxhim_results_type(res, &type);
 *         switch (type) {
 *             case HXHIM_RESULT_PUT:
 *                 // do stuff with put
 *                 break;
 *             case HXHIM_RESULT_GET:
 *             case HXHIM_RESULT_GETOP:
 *                 // do stuff with get
 *                 break;
 *             case HXHIM_RESULT_DEL:
 *                 // do stuff with del
 *                 break;
 *             default:
 *                 break;
 *         }
 *     }
 *     hxhim_results_destroy(res);
 *
 */

typedef enum hxhim_result_type_t {
    HXHIM_RESULT_NONE = 0,
    HXHIM_RESULT_PUT,
    HXHIM_RESULT_GET,
    HXHIM_RESULT_GETOP,
    HXHIM_RESULT_DEL,
    HXHIM_RESULT_SYNC,

    HXHIM_RESULT_MAX,
} hxhim_result_type_t;

extern const char *hxhim_result_type_str[];

// timestamps for a single operation
// epoch can be obtained with hxhimGetEpoch
typedef struct hxhim_result_timestamps_t {
    struct Send send;
    struct SendRecv transport;
    struct Recv recv;
} hxhim_result_timestamps_t;

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
int hxhim_result_type(hxhim_results_t *res, enum hxhim_result_type_t *type);
int hxhim_result_status(hxhim_results_t *res, int *status);                /* whether or not the results are good */
int hxhim_result_datastore(hxhim_results_t *res, int *datastore);
int hxhim_result_subject(hxhim_results_t *res, void **subject, size_t *subject_len);
int hxhim_result_predicate(hxhim_results_t *res, void **predicate, size_t *predicate_len);
// these accessor functions only work for GET results
int hxhim_result_object_type(hxhim_results_t *res, enum hxhim_type_t *object_type);
int hxhim_result_object(hxhim_results_t *res, void **object, size_t *object_len);

int hxhim_result_timestamps(hxhim_results_t *res, struct hxhim_result_timestamps_t **timestamps);

void hxhim_results_destroy(hxhim_results_t *res);

#ifdef __cplusplus
}
#endif

#endif
