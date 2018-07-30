#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include <mpi.h>

#include "hxhim/hxhim.h"
#include "print_results.h"
#include "spo_gen.h"

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // read the config
    hxhim_options_t opts;
    if (hxhim_default_config_reader(&opts, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        fprintf(stderr, "Failed to read configuration");
        return 1;
    }

    // start hxhim
    hxhim_t hx;
    if (hxhimOpen(&hx, &opts) != HXHIM_SUCCESS) {
        fprintf(stderr, "Failed to initialize hxhim\n");
        return 1;
    }

    // Generate some subject-predicate-object triples
    const size_t count = 1;
    void **subjects = NULL, **predicates = NULL, **objects = NULL;
    size_t *subject_lens = NULL, *predicate_lens = NULL, *object_lens = NULL;
    if (spo_gen_fixed(count, 100, rank, &subjects, &subject_lens, &predicates, &predicate_lens, &objects, &object_lens) != count) {
        return -1;
    }

    // PUT the key value pairs into MDHIM
    for(size_t i = 0; i < count; i++) {
        hxhimPut(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], HXHIM_BYTE_TYPE, objects[i], object_lens[i]);
        printf("Rank %d PUT {%.*s, %.*s} -> %.*s\n", rank, (int) subject_lens[i], subjects[i], (int) predicate_lens[i], predicates[i], (int) object_lens[i], objects[i]);
    }

    // GET them back, flushing only the GETs
    for(size_t i = 0; i < count; i++) {
        hxhimGet(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], HXHIM_BYTE_TYPE);
    }
    hxhim_results_t *flush_get_res = hxhimFlushGets(&hx);
    printf("GET before flushing PUTs\n");
    print_results(&hx, 1, flush_get_res);
    hxhim_results_destroy(flush_get_res);

    // GET again, but flush everything this time
    enum hxhim_type_t *bget_types = malloc(count * sizeof(enum hxhim_type_t));
    for(size_t i = 0; i < count; i++) {
        bget_types[i] = HXHIM_BYTE_TYPE;
    }

    hxhimBGet(&hx, subjects, subject_lens, predicates, predicate_lens, bget_types, count);
    hxhim_results_t *flush_all_res = hxhimFlush(&hx);
    printf("GET after flushing PUTs\n");
    print_results(&hx, 1, flush_all_res);
    hxhim_results_destroy(flush_all_res);

    spo_clean(count, subjects, subject_lens, predicates, predicate_lens, objects, object_lens);
    free(bget_types);

    MPI_Barrier(MPI_COMM_WORLD);

    hxhimClose(&hx);
    hxhim_options_destroy(&opts);

    MPI_Finalize();

    return 0;
}
