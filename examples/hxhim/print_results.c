#include <float.h>
#include <inttypes.h>
#include <stdio.h>

#include "hxhim/accessors.h"
#include "print_results.h"
#include "utils/elen.h"

void print_by_type(void *value, size_t value_len, enum hxhim_data_t type) {
    switch (type) {
        case HXHIM_DATA_INT32:
            printf("%" PRId32, * (int32_t *) value);
            break;
        case HXHIM_DATA_INT64:
            printf("%" PRId64, * (int64_t *) value);
            break;
        case HXHIM_DATA_UINT32:
            printf("%" PRId32, * (uint32_t *) value);
            break;
        case HXHIM_DATA_UINT64:
            printf("%" PRId64, * (uint64_t *) value);
            break;
        case HXHIM_DATA_FLOAT:
            printf("%.*f", FLT_DECIMAL_DIG, * (float *) value);
            break;
        case HXHIM_DATA_DOUBLE:
            printf("%.*f", DBL_DECIMAL_DIG, * (double *) value);
            break;
        case HXHIM_DATA_BYTE:
            printf("%.*s", (int) value_len, (char *) value);
            break;
        case HXHIM_DATA_POINTER:
            printf("%p", value);
            break;
        default:
            printf("Invalid Type (%zu bytes)", value_len);
            break;
    }
}

void print_results(hxhim_t *hx, const int print_rank, hxhim_results_t *results) {
    if (!results) {
        return;
    }

    HXHIM_C_RESULTS_LOOP(results) {
        if (print_rank) {
            int rank = -1;
            if (hxhimGetMPI(hx, NULL, &rank, NULL) != HXHIM_SUCCESS) {
                printf("Could not get rank\n");
            }
            else {
                printf("Rank %d ", rank);
            }
        }

        enum hxhim_op_t op;
        hxhim_result_op(results, &op);

        int status;
        hxhim_result_status(results, &status);

        int range_server;
        hxhim_result_range_server(results, &range_server);

        void *subject = NULL;
        size_t subject_len = 0;
        enum hxhim_data_t subject_type;

        void *predicate = NULL;
        size_t predicate_len = 0;
        enum hxhim_data_t predicate_type;

        if (status == HXHIM_SUCCESS) {
            hxhim_result_subject(results, &subject, &subject_len, &subject_type);
            hxhim_result_predicate(results, &predicate, &predicate_len, &subject_type);
        }

        switch (op) {
            case HXHIM_PUT:
                printf("PUT          {");
                print_by_type(subject, subject_len, subject_type);
                printf(", ");
                print_by_type(predicate, predicate_len, predicate_type);
                printf("} returned %s from range server %d\n", (status == HXHIM_SUCCESS)?"SUCCESS":"ERROR", range_server);
                break;
            case HXHIM_GET:
            case HXHIM_GETOP:
                printf("GET returned ");
                if (status == HXHIM_SUCCESS) {
                    void *object = NULL;
                    size_t object_len = 0;
                    enum hxhim_data_t object_type;
                    hxhim_result_object(results, &object, &object_len, &object_type);

                    printf("{");
                    print_by_type(subject, subject_len, subject_type);
printf(", ");
                    print_by_type(predicate, predicate_len, predicate_type);
printf("} -> ");
                    print_by_type(object, object_len, object_type);
                }
                else {
                    printf("ERROR");
                }

                printf(" from range server %d\n", range_server);
                break;
            case HXHIM_DELETE:
                printf("DEL          {");
                print_by_type(subject, subject_len, subject_type);
                printf(", ");
                print_by_type(predicate, predicate_len, predicate_type);
                printf("} returned %s from range server %d\n", (status == HXHIM_SUCCESS)?"SUCCESS":"ERROR", range_server);
                break;
            default:
                printf("Bad Operation: %d\n", (int) op);
                break;
        }
    }
}
