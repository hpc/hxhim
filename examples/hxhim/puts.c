#include <stdio.h>

#include <mpi.h>

#include "hxhim/hxhim.h"
#include "spo_gen.h"
#include "timestamps.h"

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    size_t count = 1000 * 500;
    size_t times = 2;

    // if an argument is provided, use it as the count
    if (argc > 1) {
        if (sscanf(argv[1], "%zu", &count) != 1) {
            return 1;
        }
    }

    // if another argument is provided, use it as the times to loop
    if (argc > 2) {
        if (sscanf(argv[2], "%zu", &times) != 1) {
            return 1;
        }
    }

    int size = -1;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // read the config
    hxhim_options_t opts;
    if (hxhim_default_config_reader(&opts, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        fprintf(stderr, "Failed to read configuration\n");
        return 1;
    }

    // start hxhim
    hxhim_t hx;
    if (hxhimOpen(&hx, &opts) != HXHIM_SUCCESS) {
        fprintf(stderr, "Failed to initialize hxhim\n");
        hxhim_options_destroy(&opts);
        return 1;
    }

    struct timespec epoch;
    hxhimGetEpoch(&hx, &epoch);

    long double results_duration = 0;

    // do PUTs
    for(size_t i = 0; i < times; i++) {
        // Generate some subject-predicate-object triples
        timestamp_start(gen);
        void   **subjects     = NULL, **predicates     = NULL, **objects     = NULL;
        size_t  *subject_lens = NULL,  *predicate_lens = NULL,  *object_lens = NULL;
        if (spo_gen_random(count,
                           &subjects,   &subject_lens,   8,                  8,
                           &predicates, &predicate_lens, sizeof(int),        sizeof(int),
                           &objects,    &object_lens,    3 * sizeof(double), 3 * sizeof(double))
            != count) {
            printf("Could not generate triples\n");
            return 1;
        }
        timestamp_end(gen);

        // BPUT the key value pairs into HXHIM
        timestamp_start(BPUT);
        hxhimBPutSingleType(&hx,
                            subjects, subject_lens,
                            predicates, predicate_lens,
                            HXHIM_OBJECT_TYPE_BYTE,
                            objects, object_lens,
                            count);
        timestamp_end(BPUT);

        timestamp_start(flush);
        hxhim_results_t *flush = hxhimFlush(&hx);
        timestamp_end(flush);

        long double duration = 0;
        hxhim_results_duration(flush, &duration);
        results_duration += duration;

        timestamp_start(destroy);
        hxhim_results_destroy(flush);
        timestamp_end(destroy);

        timestamp_start(clean);
        spo_clean(count,
                  &subjects,   &subject_lens,
                  &predicates, &predicate_lens,
                  &objects,    &object_lens);
        timestamp_end(clean);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    const size_t total_count = count * times;

    for(int i = 0; i < size; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        if (i == rank) {
            fprintf(stderr, "Rank %d: %zu PUTs in %.3Lf seconds (%.3Lf PUTs/sec)\n", i, total_count, results_duration, total_count / results_duration);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }

    hxhimClose(&hx);
    hxhim_options_destroy(&opts);

    MPI_Finalize();

    return 0;
}
