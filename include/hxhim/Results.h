#ifndef HXHIM_RESULTS_H
#define HXHIM_RESULTS_H

#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

enum hxhim_result_type {
    HXHIM_RESULT_NONE,
    HXHIM_RESULT_PUT,
    HXHIM_RESULT_GET,
    HXHIM_RESULT_DEL,
};

// Opaque C struct, since user will never create one themselves
typedef struct hxhim_results hxhim_results_t;

// "iterator" functions
int hxhim_results_goto_head(hxhim_results_t *res);
int hxhim_results_goto_next(hxhim_results_t *res);

// accessor functions - only operates on current result in results list
int hxhim_results_valid(hxhim_results_t *res);
int hxhim_results_type(hxhim_results_t *res, enum hxhim_result_type *type);
int hxhim_results_error(hxhim_results_t *res, int *error);
int hxhim_results_database(hxhim_results_t *res, int *database);
// these accessor functions only work for GET results
int hxhim_results_get_subject(hxhim_results_t *res, void **subject, size_t *subject_len);
int hxhim_results_get_predicate(hxhim_results_t *res, void **predicate, size_t *predicate_len);
int hxhim_results_get_object(hxhim_results_t *res, void **object, size_t *object_len);

void hxhim_results_destroy(hxhim_results_t *res);

#ifdef __cplusplus
}
#endif

#endif
