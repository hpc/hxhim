#include <iostream>
#include <sstream>

#include <mpi.h>

#include "hxhim.hpp"
#include "kv_gen.h"

static void print_results(const int rank, hxhim::Return *results) {
    if (results) {
        // iterate through each range server
        for(results->MoveToFirstRS(); results->ValidRS() == HXHIM_SUCCESS; results->NextRS()) {
            // iterate through each key value pair
            for(results->MoveToFirstKV(); results->ValidKV() == HXHIM_SUCCESS; results->NextKV()) {
                char *key; std::size_t key_len;
                results->GetKV((void **) &key, &key_len, nullptr, nullptr);
                std::cout << "Rank " << rank << " GET " << std::string(key, key_len);

                if (results->GetError() == HXHIM_SUCCESS) {
                    char *value; std::size_t value_len;
                    results->GetKV(nullptr, nullptr, (void **) &value, &value_len);
                    std::cout << " -> " << std::string(value, value_len);
                }
                else {
                    std::cout << " failed";
                }
                std::cout << " on range server " << results->GetSrc() << std::endl;
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
    const std::size_t count = 10;
    void **keys = NULL, **values = NULL;
    size_t *key_lens = NULL, *value_lens = NULL;
    kv_gen(count, 100, rank, &keys, &key_lens, &values, &value_lens);

    // start hxhim
    hxhim_t hx;
    hxhim::Open(&hx, MPI_COMM_WORLD, "mdhim.conf");

    // PUT the key value pairs into MDHIM
    for(size_t i = 0; i < count; i++) {
        hxhim::Put(&hx, keys[i], key_lens[i], values[i], value_lens[i]);
    }

    // GET them back without flushing
    for(size_t i = 0; i < count; i++) {
        hxhim::Return *results = hxhim::Get(&hx, keys[i], key_lens[i]);
        print_results(rank, results);
        delete results;
    }

    // flush the PUT stream
    if (hxhim::Flush(&hx) != HXHIM_SUCCESS) {
        std::cerr << "Rank " << rank << " failed to flush" << std::endl;
    }
    else {
        std::cerr << "Rank " << rank << " flushed" << std::endl;

        // use BGET instead of GET
        hxhim::Return *results = hxhim::BGet(&hx, keys, key_lens, count);
        print_results(rank, results);
        delete results;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    kv_clean(count, keys, key_lens, values, value_lens);

    hxhim::Close(&hx);
    MPI_Finalize();

    return 0;
}
