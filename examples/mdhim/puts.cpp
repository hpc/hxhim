#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <ctime>

#include "mdhim/mdhim.hpp"
#include "mdhim/config.h"

// A quick and dirty cleanup function
static void cleanup(mdhim_t *md, mdhim_options_t *opts) {
    mdhim::Close(md);
    mdhim_options_destroy(opts);
    MPI_Finalize();
}

int main(int argc, char *argv[]){
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    srand(time(NULL));

    // initialize options through config
    mdhim_options_t opts;
    if (mdhim_default_config_reader(&opts, MPI_COMM_WORLD) != MDHIM_SUCCESS) {
        printf("Error Reading Configuration\n");
        cleanup(NULL, &opts);
        return MDHIM_ERROR;
    }

    // initialize mdhim context
    mdhim_t md;
    if (mdhim::Init(&md, &opts) != MDHIM_SUCCESS) {
        printf("Error Initializng MDHIM\n");
        cleanup(&md, &opts);
        return MDHIM_ERROR;
    }

    const std::size_t count = MAX_BULK_OPS;

    // create the length array that will be used for both keys and values
    std::size_t *lens = new std::size_t[count]();
    for(std::size_t i = 0; i < count; i++) {
        lens[i] = sizeof(std::size_t);
    }

    // BPut repeatedly
    for(std::size_t round = 0; round < 170; round++) {
        void **kvs = new void *[count]();
        for(std::size_t i = 0; i < count; i++) {
            const std::size_t kv = round * MAX_BULK_OPS + i;

            kvs[i] = ::operator new(sizeof(int));
            memcpy(kvs[i], &kv, sizeof(kv));
        }

        delete mdhim::BPut(&md, nullptr, kvs, lens, kvs, lens, count);
        delete [] kvs;
    }

    cleanup(&md, &opts);

    return 0;
}
