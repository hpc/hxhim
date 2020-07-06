#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include <mpi.h>

#include "hxhim/hxhim.h"
#include "print_results.h"
#include "spo_gen.h"

const size_t count = 10;
const size_t bufsize = 100;

void ordered_print(MPI_Comm comm, const int rank, const int size, hxhim_t * hx, hxhim_results_t *res) {
    for(int i = 0; i < size; i++) {
        MPI_Barrier(comm);
        if (i == rank) {
            print_results(hx, 1, res);
        }
        MPI_Barrier(comm);
    }
}

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

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

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        printf("PUT key value pairs\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);

    // PUT the key value pairs into HXHIM
    for(size_t i = 0; i < count; i++) {
        hxhimPut(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], HXHIM_BYTE_TYPE, objects[i], object_lens[i]);
        printf("Rank %d PUT          {%.*s, %.*s} -> %.*s\n", rank,
               (int) subject_lens[i],   (char *) subjects[i],
               (int) predicate_lens[i], (char *) predicates[i],
               (int) object_lens[i],    (char *) objects[i]);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        printf("GET before flushing PUTs\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);

    // GET them back, flushing only the GETs
    // this will likely return errors, since not all of the PUTs will have completed
    for(size_t i = 0; i < count; i++) {
        hxhimGet(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], HXHIM_BYTE_TYPE);
    }

    hxhim_results_t *flush_gets_early = hxhimFlushGets(&hx);
    ordered_print(MPI_COMM_WORLD, rank, size, &hx, flush_gets_early);
    hxhim_results_destroy(flush_gets_early);

    // flush PUTs
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        printf("Flush PUTs\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);

    hxhim_results_t *flush_puts = hxhimFlush(&hx);
    ordered_print(MPI_COMM_WORLD, rank, size, &hx, flush_puts);
    hxhim_results_destroy(flush_puts);

    MPI_Barrier(MPI_COMM_WORLD);

    // GET again, now that all PUTs have completed
    for(size_t i = 0; i < count; i++) {
        hxhimGet(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], HXHIM_BYTE_TYPE);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        printf("GET after flushing PUTs\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);

    hxhim_results_t *flush_gets = hxhimFlush(&hx);
    ordered_print(MPI_COMM_WORLD, rank, size, &hx, flush_gets);
    hxhim_results_destroy(flush_gets);

    // clean up
    spo_clean(count, subjects, subject_lens, predicates, predicate_lens, objects, object_lens);

    MPI_Barrier(MPI_COMM_WORLD);

    hxhimClose(&hx);
    hxhim_options_destroy(&opts);

    MPI_Finalize();

    return 0;
}
