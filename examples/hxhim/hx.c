#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include <mpi.h>

#include "hxhim.h"

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Generate some key value pairs
    const size_t count = 10;
    void **keys = (void **) calloc(count, sizeof(void *));
    size_t *key_lens = (size_t *) calloc(count, sizeof(size_t));
    void **values = (void **) calloc(count, sizeof(void *));
    size_t *value_lens = (size_t *) calloc(count, sizeof(size_t));

    const size_t bufsize = 100;
    for(size_t i = 0; i < count; i++) {
        keys[i] = calloc(bufsize, sizeof(char));
        key_lens[i] = snprintf(keys[i], bufsize, "key%d%zu", rank, i);
        values[i] = calloc(bufsize, sizeof(char));
        value_lens[i] = snprintf(values[i], bufsize, "value%d%zu", rank, i);
    }

    // start hxhim
    hxhim_t hx;
    hxhimOpen(&hx, MPI_COMM_WORLD, "mdhim.conf");

    // PUT the key value pairs into MDHIM
    for(size_t i = 0; i < count; i++) {
        hxhimPut(&hx, (void *)keys[i], key_lens[i], (void *)values[i], value_lens[i]);
    }

    // BGET them back
    hxhimBGet(&hx, (void *)keys, key_lens, count);

    // DEL the key value pairs
    for(size_t i = 0; i < count; i++) {
        hxhimDelete(&hx, (void *)keys[i], key_lens[i]);
    }

    // try to GET the key value pairs again
    for(size_t i = 0; i < count; i++) {
        hxhimGet(&hx, (void *)keys[i], key_lens[i]);
    }

    // perform the queued operations
    hxhim_return_t *results = hxhimFlush(&hx);

    // iterate through the results
    while (results) {
        enum hxhim_work_op op;
        hxhim_return_get_op(results, &op);

        // start at the first range server
        hxhim_return_move_to_first_rs(results);

        // iterate through the values
        int valid_rs = HXHIM_ERROR;
        while ((hxhim_return_valid_rs(results, &valid_rs) == HXHIM_SUCCESS) && (valid_rs == HXHIM_SUCCESS)) {
            int src = -1;
            hxhim_return_get_src(results, &src);

            int error = HXHIM_ERROR;
            hxhim_return_get_error(results, &error);
            switch (op) {
                case HXHIM_PUT:
                    printf("Rank %d PUT returned status %d on range server %d\n", rank, error, src);
                    break;
                case HXHIM_GET:
                    if (error == HXHIM_SUCCESS) {
                        // start at the first key value pair
                        hxhim_return_move_to_first_kv(results);

                        // iterate through the key value pairs
                        int valid_kv = HXHIM_ERROR;
                        while ((hxhim_return_valid_kv(results, &valid_kv) == HXHIM_SUCCESS) && (valid_kv == HXHIM_SUCCESS)) {
                            char *key, *value;
                            size_t key_len, value_len;
                            if (hxhim_return_get_kv(results, (void **) &key, &key_len, (void **) &value, &value_len) == HXHIM_SUCCESS) {
                                // https://stackoverflow.com/a/3767300
                                printf("Rank %d GET %.*s -> %.*s on range server %d\n", rank, (int) key_len, key, (int) value_len, value, src);
                            }

                            hxhim_return_next_kv(results);
                        }
                    }
                    else {
                        printf("Rank %d GET failed on range server %d\n", rank, src);
                    }
                    break;
                case HXHIM_DEL:
                    printf("Rank %d DEL returned status %d on range server %d\n", rank, error, src);
                    break;
                default:
                    printf("Found a bad return value with operation %d\n", (int) op);
                    break;
            }

            hxhim_return_next_rs(results);
        }

        hxhim_return_t *next = NULL;
        hxhim_return_next(results, &next);
        hxhim_return_destroy(results);
        results = next;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    for(size_t i = 0; i < count; i++) {
        free(keys[i]);
        free(values[i]);
    }

    free(keys);
    free(key_lens);
    free(values);
    free(value_lens);

    hxhimClose(&hx);
    MPI_Finalize();

    return 0;
}
