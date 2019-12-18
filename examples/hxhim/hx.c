#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include <mpi.h>

#include "hxhim/hxhim.h"
#include "print_results.h"
#include "spo_gen.h"

const size_t count = 1;
const size_t bufsize = 100;

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // read the config
    hxhim_options_t opts;
    if (hxhim_default_config_reader(&opts, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        if (rank == 0) {
            fprintf(stderr, "Failed to read configuration\n");
        }
        return 1;
    }

    // start hxhim
    hxhim_t hx;
    if (hxhimOpen(&hx, &opts) != HXHIM_SUCCESS) {
        if (rank == 0) {
            fprintf(stderr, "Failed to initialize hxhim\n");
        }
        return 1;
    }

    // Generate some subject-predicate-object triples
    void **subjects = NULL, **predicates = NULL, **objects = NULL;
    size_t *subject_lens = NULL, *predicate_lens = NULL, *object_lens = NULL;
    if (spo_gen_fixed(count, bufsize, rank, &subjects, &subject_lens, &predicates, &predicate_lens, &objects, &object_lens) != count) {
        return -1;
    }

    // PUT the key value pairs into MDHIM
    for(size_t i = 0; i < count; i++) {
        hxhimPut(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], HXHIM_BYTE_TYPE, objects[i], object_lens[i]);
        printf("Rank %d PUT {%.*s, %.*s} -> %.*s\n", rank, (int) subject_lens[i], (char *) subjects[i], (int) predicate_lens[i], (char *) predicates[i], (int) object_lens[i], (char *) objects[i]);
    }

    // GET them back, flushing only the GETs
    void **get_objects = malloc(count * sizeof(void *));
    size_t **get_object_lens = malloc(count * sizeof(size_t *));
    for(size_t i = 0; i < count; i++) {
        get_objects[i] = malloc(bufsize);
        get_object_lens[i] = malloc(sizeof(size_t));
        *(get_object_lens[i]) = bufsize;
        hxhimGet2(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], HXHIM_BYTE_TYPE, get_objects[i], get_object_lens[i]);
    }
    hxhim_results_t *flush_get_res = hxhimFlushGets2(&hx);
    printf("GET before flushing PUTs\n");
    print_results(&hx, 1, flush_get_res);
    hxhim_results_destroy(flush_get_res);

    // GET again, but flush everything this time
    enum hxhim_type_t *bget_types = malloc(count * sizeof(enum hxhim_type_t));
    for(size_t i = 0; i < count; i++) {
        bget_types[i] = HXHIM_BYTE_TYPE;
    }

    hxhimBGet2(&hx, subjects, subject_lens, predicates, predicate_lens, bget_types, get_objects, get_object_lens, count);
    hxhim_results_t *flush_all_res = hxhimFlush(&hx);
    printf("GET after flushing PUTs\n");
    print_results(&hx, 1, flush_all_res);
    hxhim_results_destroy(flush_all_res);

    for(size_t i = 0; i < count; i++) {
        free(get_objects[i]);
        free(get_object_lens[i]);
    }
    free(get_objects);
    free(get_object_lens);

    spo_clean(count, subjects, subject_lens, predicates, predicate_lens, objects, object_lens);
    free(bget_types);

    MPI_Barrier(MPI_COMM_WORLD);

    hxhimClose(&hx);
    hxhim_options_destroy(&opts);

    MPI_Finalize();

    return 0;
}
