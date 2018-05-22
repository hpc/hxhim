#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#include <mpi.h>

#include "hxhim.h"
#include "kv_gen.h"

static void print_results(const int rank, hxhim_return_t *results) {
    // move the internal index to the first range server
    if (hxhim_return_move_to_first_rs(results) == HXHIM_SUCCESS) {
        // iterate through each range server
        for(int valid_rs; (hxhim_return_valid_rs(results, &valid_rs) == HXHIM_SUCCESS) && (valid_rs == HXHIM_SUCCESS); hxhim_return_next_rs(results)) {
            // move the internal index to the beginning of the key value pairs list
            if (hxhim_return_move_to_first_kv(results) == HXHIM_SUCCESS) {
                // make sure the return value can be obtained
                int error = HXHIM_SUCCESS;
                if (hxhim_return_get_error(results, &error) == HXHIM_SUCCESS) {
                    // iterate through each key value pair
                    for(int valid_kv; (hxhim_return_valid_kv(results, &valid_kv) == HXHIM_SUCCESS) && (valid_kv == HXHIM_SUCCESS); hxhim_return_next_kv(results)) {
                        // get the key
                        char *key; size_t key_len;
                        hxhim_return_get_kv(results, (void **) &key, &key_len, NULL, NULL);
                        printf("Rank %d GET %.*s ", rank, (int) key_len, (char *) key);

                        // if there was no error, get the value
                        if (error == HXHIM_SUCCESS) {
                            char *value; size_t value_len;
                            hxhim_return_get_kv(results, NULL, NULL, (void **) &value, &value_len);
                            printf("-> %.*s", (int) value_len, (char *) value);
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
    }
}

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Generate some key value pairs
    const size_t count = 10;
    void **keys = NULL, **values = NULL;
    size_t *key_lens = NULL, *value_lens = NULL;
    if (kv_gen(count, 100, rank, &keys, &key_lens, &values, &value_lens) != count) {
        return -1;
    }

    // start hxhim
    hxhim_t hx;
    hxhimOpen(&hx, MPI_COMM_WORLD, "mdhim.conf");

    // PUT the key value pairs into MDHIM
    for(size_t i = 0; i < count; i++) {
        hxhimPut(&hx, keys[i], key_lens[i], values[i], value_lens[i]);
        printf("Rank %d PUT %.*s -> %.*s\n", rank, (int) key_lens[i], (char *) keys[i], (int) value_lens[i], (char *) values[i]);
    }

    // GET them back, flushing only the GETs
    for(size_t i = 0; i < count; i++) {
         hxhimGet(&hx, keys[i], key_lens[i]);
    }
    hxhim_return_t *flush_get_res = hxhimFlushGets(&hx);
    print_results(rank, flush_get_res);
    hxhim_return_destroy(flush_get_res);

    // GET again, but flush everything this time
    for(size_t i = 0; i < count; i++) {
        hxhimGet(&hx, keys[i], key_lens[i]);
    }

    hxhim_return_t **flush_all_res = hxhimFlush(&hx);
    for(int i = 0; i < HXHIM_RESULTS_SIZE; i++) {
        print_results(rank, flush_all_res[i]);
    }
    hxhimDestroyFlush(flush_all_res);

    MPI_Barrier(MPI_COMM_WORLD);

    kv_clean(count, keys, key_lens, values, value_lens);

    hxhimClose(&hx);
    MPI_Finalize();

    return 0;
}
