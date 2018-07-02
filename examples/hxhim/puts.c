#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <mpi.h>

#include "hxhim/hxhim.h"
#include "spo_gen.h"

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    srand(time(NULL));

    // start hxhim
    hxhim_t hx;
    if (hxhimOpen(&hx, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        printf("Could not start HXHIM\n");
        return -1;
    }

    // Generate some subject-predicate-object triples
    const size_t count = HXHIM_MAX_BULK_PUT_OPS * 10;
    void **subjects = NULL, **predicates = NULL, **objects = NULL;
    size_t *subject_lens = NULL, *predicate_lens = NULL, *object_lens = NULL;
    if (spo_gen_random(count, 64, 64, &subjects, &subject_lens, &predicates, &predicate_lens, &objects, &object_lens) != count) {
        printf("Could not generate triples\n");
        return -1;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // BPUT the key value pairs into HXHIM
    hxhimBPut(&hx, subjects, subject_lens, predicates, predicate_lens, objects, object_lens, count);
    hxhim_results_t *flush_all_res = hxhimFlush(&hx);
    hxhim_results_destroy(flush_all_res);
    spo_clean(count, subjects, subject_lens, predicates, predicate_lens, objects, object_lens);

    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0) {
        long double *put_times = calloc(size, sizeof(long double));
        size_t *num_puts = calloc(size, sizeof(size_t));
        long double *get_times = calloc(size, sizeof(long double));
        size_t *num_gets = calloc(size, sizeof(size_t));

        hxhimGetStats(&hx, 0, 1, put_times, 1, num_puts, 1, get_times, 1, num_gets);

        size_t total_puts = 0;       // total number of PUTs across all ranks
        long double put_rates = 0;   // sum of all PUT rates
        size_t total_gets = 0;       // total number of GETs across all ranks
        long double get_rates = 0;   // sum of all GET rates

        printf("Stats:\n");
        printf("Rank PUTs seconds GETs seconds\n");
        for(int i = 0; i < size; i++) {
            printf("%*d %zu %Lf %zu %Lf\n", (int) ceil(log10(size)), i, num_puts[i], put_times[i], num_gets[i], get_times[i]);

            if (num_puts[i]) {
                total_puts += num_puts[i];
                put_rates += num_puts[i] / put_times[i];
            }

            if (num_gets[i]) {
                total_gets += num_gets[i];
                get_rates += num_gets[i] / get_times[i];
            }
        }

        free(put_times);
        free(num_puts);
        free(get_times);
        free(num_gets);

        printf("Total:   %zu PUTs     %zu GETs\n", total_puts, total_gets);
        printf("Average: %Lf PUTs/sec %Lf GETs/sec\n", put_rates, get_rates);
    }
    else {
        hxhimGetStats(&hx, 0, 1, NULL, 1, NULL, 1, NULL, 1, NULL);
    }

    hxhimClose(&hx);
    MPI_Finalize();

    return 0;
}
