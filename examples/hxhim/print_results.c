#include "print_results.h"

void print_results(const int rank, hxhim_results_t *results) {
    if (!results) {
        return;
    }

    for(hxhim_results_goto_head(results); hxhim_results_valid(results) == HXHIM_SUCCESS; hxhim_results_goto_next(results)) {
        if (rank > -1) {
            printf("Rank %d ", rank);
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

                    void *object = NULL;
                    size_t object_len = 0;
                    hxhim_results_get_object(results, &object, &object_len);

                    printf("{%.*s, %.*s} -> %.*s",
                           (int) subject_len, (char *) subject,
                           (int) predicate_len, (char *) predicate,
                           (int) object_len, (char *) object);
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
