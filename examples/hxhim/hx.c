#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include <mpi.h>

#include "hxhim.h"
#include "spo_gen.h"

static void print_results(const int rank, hxhim_return_t *results) {
    // move the internal index to the first range server
    while (hxhim_return_move_to_first_rs(results) == HXHIM_SUCCESS) {
        // iterate through each range server
        for(int valid_rs; (hxhim_return_valid_rs(results, &valid_rs) == HXHIM_SUCCESS) && (valid_rs == HXHIM_SUCCESS); hxhim_return_next_rs(results)) {
            // move the internal index to the beginning of the key value pairs list
            if (hxhim_return_move_to_first_spo(results) == HXHIM_SUCCESS) {
                // make sure the return value can be obtained
                int error = HXHIM_SUCCESS;
                if (hxhim_return_get_error(results, &error) == HXHIM_SUCCESS) {
                    // iterate through each key value pair
                    for(int valid_spo; (hxhim_return_valid_spo(results, &valid_spo) == HXHIM_SUCCESS) && (valid_spo == HXHIM_SUCCESS); hxhim_return_next_spo(results)) {
                        // get the key
                        char *subject; size_t subject_len;
                        char *predicate; size_t predicate_len;
                        hxhim_return_get_spo(results, (void **) &subject, &subject_len, (void **) &predicate, &predicate_len, NULL, NULL);
                        printf("Rank %d GET {%.*s, %.*s} ", rank, (int) subject_len, (char *) subject, (int) predicate_len, (char *) predicate);

                        // if there was no error, get the value
                        if (error == HXHIM_SUCCESS) {
                            char *object; size_t object_len;
                            hxhim_return_get_spo(results, NULL, NULL, NULL, NULL, (void **) &object, &object_len);
                            printf("-> %.*s", (int) object_len, (char *) object);
                        }
                        else {
                            printf("failed");
                        }

                        int src = -1;
                        hxhim_return_get_src(results, &src);
                        printf(" on range server %d\n", src);
                    }
                }
            }
        }
        hxhim_return_next(results);
    }
}

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // start hxhim
    hxhim_t hx;
    if (hxhimOpen(&hx, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        fprintf(stderr, "Failed to initialize hxhim\n");
        return 1;
    }

    // Generate some subject-predicate-object triples
    const size_t count = 10;
    void **subjects = NULL, **predicates = NULL, **objects = NULL;
    size_t *subject_lens = NULL, *predicate_lens = NULL, *object_lens = NULL;
    if (spo_gen_fixed(count, 100, rank, &subjects, &subject_lens, &predicates, &predicate_lens, &objects, &object_lens) != count) {
        return -1;
    }

    // PUT the key value pairs into MDHIM
    for(size_t i = 0; i < count; i++) {
        hxhimPut(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], objects[i], object_lens[i]);
        printf("Rank %d PUT {%.*s, %.*s} -> %.*s\n", rank, (int) subject_lens[i], subjects[i], (int) predicate_lens[i], predicates[i], (int) object_lens[i], objects[i]);
    }

    // GET them back, flushing only the GETs
    for(size_t i = 0; i < count; i++) {
         hxhimGet(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i]);
    }
    hxhim_return_t *flush_get_res = hxhimFlushGets(&hx);
    printf("GET before flushing PUTs\n");
    print_results(rank, flush_get_res);
    hxhim_return_destroy(flush_get_res);

    // GET again, but flush everything this time
    hxhimBGet(&hx, subjects, subject_lens, predicates, predicate_lens, count);
    hxhim_return_t *flush_all_res = hxhimFlush(&hx);
    printf("GET after flushing PUTs\n");
    print_results(rank, flush_all_res);
    hxhim_return_destroy(flush_all_res);

    MPI_Barrier(MPI_COMM_WORLD);

    spo_clean(count, subjects, subject_lens, predicates, predicate_lens, objects, object_lens);

    hxhimClose(&hx);
    MPI_Finalize();

    return 0;
}
