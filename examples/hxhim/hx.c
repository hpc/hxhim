#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include <mpi.h>

#include "hxhim/hxhim.h"
#include "utils/elen.h"
#include "print_results.h"
#include "spo_gen.h"
#include "timestamps.h"

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
    if (argc < 3) {
        fprintf(stderr, "Syntax: %s count print?\n", argv[0]);
        return 1;
    }

    int provided = 0;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size = -1;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    size_t count = 0;
    if (sscanf(argv[1], "%zu", &count) != 1) {
        fprintf(stderr, "Error: Could not parse count argument: %s\n", argv[1]);
        return 1;
    }

    int print = 0;
    if (sscanf(argv[2], "%d", &print) != 1) {
        fprintf(stderr, "Error: Could not parse print argument: %s\n", argv[2]);
        return 1;
    }

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

    struct timespec epoch;
    hxhimGetEpoch(&hx, &epoch);

    // Generate some subject-predicate-object triples
    timestamp_start(gen);
    void **subjects = NULL, **predicates = NULL;
    size_t *subject_lens = NULL, *predicate_lens = NULL;
    if (spo_gen_fixed(count, bufsize, rank, &subjects, &subject_lens, &predicates, &predicate_lens, NULL, NULL) != count) {
        return -1;
    }

    double *doubles = malloc(sizeof(double) * count);
    for(size_t i = 0; i < count; i++) {
        doubles[i] = rand();
        doubles[i] /= rand();
        doubles[i] *= rand();
        doubles[i] /= rand();
    }
    timestamp_end(gen);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        printf("PUT key value pairs\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);

    // PUT the key value pairs into HXHIM
    timestamp_start(put);
    for(size_t i = 0; i < count; i++) {
        hxhimPutDouble(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], &doubles[i]);
        if (print) {
            printf("Rank %d PUT          {%.*s, %.*s} -> %f\n", rank,
                   (int) subject_lens[i],   (char *) subjects[i],
                   (int) predicate_lens[i], (char *) predicates[i],
                   doubles[i]);
        }
    }
    timestamp_end(put);

    barrier;

    if (rank == 0) {
        printf("GET before flushing PUTs\n");
    }

    barrier;

    // GET them back, flushing only the GETs
    // this will likely return errors, since not all of the PUTs will have completed
    for(size_t i = 0; i < count; i++) {
        hxhimGetDouble(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i]);
    }

    hxhim_results_t *flush_gets_early = hxhimFlushGets(&hx);
    if (print) {
        ordered_print(MPI_COMM_WORLD, rank, size, &hx, flush_gets_early);
    }
    hxhim_results_destroy(flush_gets_early);

    // flush PUTs
    barrier;

    if (rank == 0) {
        printf("Flush PUTs\n");
    }

    barrier;

    timestamp_start(flush_put);
    hxhim_results_t *flush_puts = hxhimFlushPuts(&hx);
    timestamp_end(flush_put);

    barrier;

    if (print) {
        ordered_print(MPI_COMM_WORLD, rank, size, &hx, flush_puts);
    }

    uint64_t duration = 0;
    hxhim_results_duration(flush_puts, &duration);

    for(int i = 0; i < size; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        if (i == rank) {
            fprintf(stderr, "Rank %d: %zu PUTs in %.3f seconds (%.3f PUTs/sec)\n", i, count, duration / 1e9, count * 1e9 / duration);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }

    timestamp_start(destroy);
    hxhim_results_destroy(flush_puts);
    timestamp_end(destroy);

    // GET again, now that all PUTs have completed
    for(size_t i = 0; i < count; i++) {
        hxhimGetDouble(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i]);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        printf("GET after flushing PUTs\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);

    hxhim_results_t *flush_gets = hxhimFlush(&hx);
    if (print) {
        ordered_print(MPI_COMM_WORLD, rank, size, &hx, flush_gets);
    }
    hxhim_results_destroy(flush_gets);

    // clean up
    timestamp_start(cleanup);
    free(doubles);
    spo_clean(count, &subjects, &subject_lens, &predicates, &predicate_lens, NULL, NULL);
    timestamp_end(cleanup);

    MPI_Barrier(MPI_COMM_WORLD);

    hxhimClose(&hx);
    hxhim_options_destroy(&opts);

    MPI_Finalize();

    return 0;
}
