#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mpi.h>

#include "hxhim/hxhim.h"
#include "spo_gen.h"

int main(int argc, char *argv[]) {
    size_t count = 1000 * 500;
    size_t times = 2;

    if (argc > 1) {
        if (!(count = atoi(argv[1]))) {
            return 1;
        }
    }

    if (argc > 2) {
        if (!(times = atoi(argv[2]))) {
            return 1;
        }
    }

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    srand(time(NULL));

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
        hxhim_options_destroy(&opts);
        return 1;
    }

    // do PUTs
    for(size_t i = 0; i < times; i++) {
        // Generate some subject-predicate-object triples
        void **subjects = NULL, **predicates = NULL, **objects = NULL;
        size_t *subject_lens = NULL, *predicate_lens = NULL, *object_lens = NULL;
        /* if (spo_gen_fixed(count, 100, rank, */
        /*                   &subjects, &subject_lens, */
        /*                   &predicates, &predicate_lens, */
        /*                   &objects, &object_lens) != count) { */
        if (spo_gen_random(count,
                           8, 8, &subjects, &subject_lens,
                           sizeof(int), sizeof(int), &predicates, &predicate_lens,
                           3 * sizeof(double), 3 * sizeof(double), &objects, &object_lens) != count) {
            printf("Could not generate triples\n");
            return 1;
        }

        // BPUT the key value pairs into HXHIM
        hxhimBPutSingleType(&hx, subjects, subject_lens, predicates, predicate_lens, HXHIM_BYTE_TYPE, objects, object_lens, count);
        hxhim_results_t *flush_all_res = hxhimFlush(&hx);
        hxhim_results_destroy(&hx, flush_all_res);
        spo_clean(count, subjects, subject_lens, predicates, predicate_lens, objects, object_lens);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0) {
        long double *put_times = calloc(size, sizeof(long double));
        size_t      *num_puts  = calloc(size, sizeof(size_t));
        long double *min_bput  = calloc(size, sizeof(long double));
        long double *avg_bput  = calloc(size, sizeof(long double));
        long double *max_bput  = calloc(size, sizeof(long double));

        hxhimGetStats        (&hx, 0, 1, put_times, 1, num_puts, 0, NULL, 0, NULL);
        hxhimGetMinFilled    (&hx, 0, 1, min_bput,  0, NULL,     0, NULL, 0, NULL);
        hxhimGetAverageFilled(&hx, 0, 1, avg_bput,  0, NULL,     0, NULL, 0, NULL);
        hxhimGetMaxFilled    (&hx, 0, 1, max_bput,  0, NULL,     0, NULL, 0, NULL);

        size_t num_srvs        = 0;   // the number of server that actually did PUTs
        size_t total_puts      = 0;   // total number of PUTs across all ranks
        long double put_rates  = 0;   // sum of all PUT rates
        long double min_filled = 0;
        long double avg_filled = 0;
        long double max_filled = 0;

        printf("Stats:\n");
        printf("Rank PUTs seconds\n");
        for(int i = 0; i < size; i++) {
            printf("%*d %zu %Lf\n", (int) ceil(log10(size)), i, num_puts[i], put_times[i]);

            if (num_puts[i]) {
                num_srvs++;
                total_puts += num_puts[i];
                put_rates  += num_puts[i] / put_times[i];
                min_filled += min_bput[i];
                avg_filled += avg_bput[i];
                max_filled += max_bput[i];
            }
        }

        free(put_times);
        free(num_puts);
        free(min_bput);
        free(avg_bput);
        free(max_bput);

        printf("Total: %zu PUTs\n", total_puts);
        printf("Rate:  %Lf PUTs/sec\n", put_rates);
        printf("Number of servers that did work: %zu\n", num_srvs);
        printf("BPUT Average Minimum Filled: %.2Lf%%\n", min_filled * 100 / num_srvs);
        printf("BPUT Average Average Filled: %.2Lf%%\n", avg_filled * 100 / num_srvs);
        printf("BPUT Average Maximum Filled: %.2Lf%%\n", max_filled * 100 / num_srvs);
    }
    else {
        hxhimGetStats        (&hx, 0, 1, NULL, 1, NULL, 0, NULL, 0, NULL);
        hxhimGetMinFilled    (&hx, 0, 1, NULL, 0, NULL, 0, NULL, 0, NULL);
        hxhimGetAverageFilled(&hx, 0, 1, NULL, 0, NULL, 0, NULL, 0, NULL);
        hxhimGetMaxFilled    (&hx, 0, 1, NULL, 0, NULL, 0, NULL, 0, NULL);
    }

    hxhimClose(&hx);
    hxhim_options_destroy(&opts);

    MPI_Finalize();

    return 0;
}
