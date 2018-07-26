#include <inttypes.h>

#include "print_results.h"

static void print_by_type(hxhim_spo_type_t type, void *value, size_t value_len) {
    switch (type) {
        case HXHIM_SPO_INT_TYPE:
            printf("%d", * (int *) value);
            break;
        case HXHIM_SPO_SIZE_TYPE:
            printf("%zu", * (size_t *) value);
            break;
        case HXHIM_SPO_INT64_TYPE:
            printf("%" PRId64, * (int64_t *) value);
            break;
        case HXHIM_SPO_FLOAT_TYPE:
            printf("%f", * (float *) value);
            break;
        case HXHIM_SPO_DOUBLE_TYPE:
            printf("%f", * (double *) value);
            break;
        case HXHIM_SPO_BYTE_TYPE:
            printf("%.*s", (int) value_len, (char *) value);
            break;
    }
}

void print_results(hxhim_t *hx, const int print_rank, hxhim_results_t *results) {
    if (!results) {
        return;
    }

    for(hxhim_results_goto_head(results); hxhim_results_valid(results) == HXHIM_SUCCESS; hxhim_results_goto_next(results)) {
        if (print_rank) {
            printf("Rank %d ", hx->mpi.rank);
        }

        enum hxhim_result_type type;
        hxhim_results_type(results, &type);

        int error;
        hxhim_results_error(results, &error);

        int database;
        hxhim_results_database(results, &database);

        switch (type) {
            case HXHIM_RESULT_PUT:
                printf("PUT returned %s from database %d\n", (error == HXHIM_SUCCESS)?"SUCCESS":"ERROR", database);
                break;
            case HXHIM_RESULT_GET:
                printf("GET returned ");
                if (error == HXHIM_SUCCESS) {
                    void *subject = NULL;
                    size_t subject_len = 0;
                    hxhim_results_get_subject(results, &subject, &subject_len);

                    void *predicate = NULL;
                    size_t predicate_len = 0;
                    hxhim_results_get_predicate(results, &predicate, &predicate_len);

                    hxhim_spo_type_t object_type;
                    hxhim_results_get_object_type(results, &object_type);
                    void *object = NULL;
                    size_t object_len = 0;
                    hxhim_results_get_object(results, &object, &object_len);

                    printf("{%.*s, %.*s} -> ", (int) subject_len, subject, (int) predicate_len, predicate);
                    print_by_type(object_type, object, object_len);
                }
                else {
                    printf("ERROR");
                }

                printf(" from database %d\n", database);
                break;
            case HXHIM_RESULT_DEL:
                printf("DEL returned %s from database %d\n", (error == HXHIM_SUCCESS)?"SUCCESS":"ERROR", database);
                break;
            default:
                printf("Bad Type: %d\n", (int) type);
                break;
        }
    }
}
