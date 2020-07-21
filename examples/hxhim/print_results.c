#include <inttypes.h>

#include "print_results.h"
#include "hxhim/accessors.h"
#include "utils/elen.h"

static void print_by_type(enum hxhim_type_t type, void *value, size_t value_len) {
    switch (type) {
        case HXHIM_INT_TYPE:
            printf("%d", * (int *) value);
            break;
        case HXHIM_SIZE_TYPE:
            printf("%zu", * (size_t *) value);
            break;
        case HXHIM_INT64_TYPE:
            printf("%" PRId64, * (int64_t *) value);
            break;
        case HXHIM_FLOAT_TYPE:
            printf("%f", * (float *) value);
            break;
        case HXHIM_DOUBLE_TYPE:
            printf("%f", * (double *) value);
            break;
        case HXHIM_BYTE_TYPE:
            printf("%.*s", (int) value_len, (char *) value);
            break;
        case HXHIM_INVALID_TYPE:
        default:
            printf("Invalid Type (%zu bytes)", value_len);
            break;
    }
}

void print_results(hxhim_t *hx, const int print_rank, hxhim_results_t *results) {
    if (!results) {
        return;
    }

    for(hxhim_results_goto_head(results); hxhim_results_valid_iterator(results) == HXHIM_SUCCESS; hxhim_results_goto_next(results)) {
        if (print_rank) {
            int rank = -1;
            if (hxhimGetMPI(hx, NULL, &rank, NULL) != HXHIM_SUCCESS) {
                printf("Could not get rank\n");
            }
            else {
                printf("Rank %d ", rank);
            }
        }

        enum hxhim_result_type_t type;
        hxhim_result_type(results, &type);

        int status;
        hxhim_result_status(results, &status);

        int datastore;
        hxhim_result_datastore(results, &datastore);

        void *subject = NULL;
        size_t subject_len = 0;

        void *predicate = NULL;
        size_t predicate_len = 0;

        if (status == HXHIM_SUCCESS) {
            hxhim_result_subject(results, &subject, &subject_len);
            hxhim_result_predicate(results, &predicate, &predicate_len);
        }

        switch (type) {
            case HXHIM_RESULT_PUT:
                printf("PUT          {%.*s, %.*s} returned %s from datastore %d\n", (int) subject_len, (char *) subject, (int) predicate_len, (char *) predicate, (status == HXHIM_SUCCESS)?"SUCCESS":"ERROR", datastore);
                break;
            case HXHIM_RESULT_GET:
            case HXHIM_RESULT_GETOP:
                printf("GET returned ");
                if (status == HXHIM_SUCCESS) {
                    enum hxhim_type_t object_type;
                    hxhim_result_object_type(results, &object_type);
                    void *object = NULL;
                    size_t object_len = 0;
                    hxhim_result_object(results, &object, &object_len);

                    printf("{%.*s, %.*s} -> ", (int) subject_len, (char *) subject, (int) predicate_len, (char *) predicate);
                    print_by_type(object_type, object, object_len);
                }
                else {
                    printf("ERROR");
                }

                printf(" from datastore %d\n", datastore);
                break;
            case HXHIM_RESULT_DEL:
                printf("DEL          {%.*s, %.*s} returned %s from datastore %d\n", (int) subject_len, (char *) subject, (int) predicate_len, (char *) predicate, (status == HXHIM_SUCCESS)?"SUCCESS":"ERROR", datastore);
                break;
            default:
                printf("Bad Type: %d\n", (int) type);
                break;
        }
    }
}
