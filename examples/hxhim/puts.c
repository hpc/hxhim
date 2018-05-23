#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <mpi.h>

#include "hxhim.h"
#include "kv_gen.h"

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Generate some key value pairs
    const size_t count = 500000;
    void **keys = NULL, **values = NULL;
    size_t *key_lens = NULL, *value_lens = NULL;
    if (kv_gen(count, 100, rank, &keys, &key_lens, &values, &value_lens) != count) {
        return -1;
    }

    // start hxhim
    hxhim_t hx;
    hxhimOpen(&hx, MPI_COMM_WORLD, "mdhim.conf");

    MPI_Barrier(MPI_COMM_WORLD);

    // BPUT the key value pairs into MDHIM
    hxhimBPut(&hx, keys, key_lens, values, value_lens, count);

    const time_t start = time(NULL);
    hxhim_return_t *flush_all_res = hxhimFlush(&hx);
    const time_t end = time(NULL);

    MPI_Barrier(MPI_COMM_WORLD);

    const double diff = difftime(end, start);
    double total = 0;
    MPI_Reduce(&diff, &total, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        printf("%zu PUTs done with %f PUTs/sec on average\n", count * size, count * size * size / total);
    }

    hxhim_return_destroy(flush_all_res);
    kv_clean(count, keys, key_lens, values, value_lens);

    hxhimClose(&hx);
    MPI_Finalize();

    return 0;
}
