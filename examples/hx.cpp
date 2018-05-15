#include <sstream>

#include <mpi.h>

#include "hxhim.hpp"
#include "hxhim_private.hpp"
#include "mdhim.h"
#include "mdhim_options.h"

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    hxhim_session_t hx;
    hxhim::open(&hx, MPI_COMM_WORLD, "mdhim.conf");

    std::pair<std::string, std::string> kvs[10];
    for(int i = 0; i < 10; i++) {
        std::stringstream key, value;
        key << "key" << rank << i;
        value << "value" << rank << i;
        kvs[i].first = key.str();
        kvs[i].second = value.str();
    }

    for(std::pair<std::string, std::string> const & kv : kvs) {
        hxhim::put(&hx, (void *)kv.first.c_str(), kv.first.size(), (void *)kv.second.c_str(), kv.second.size());
    }

    for(std::pair<std::string, std::string> const & kv : kvs) {
        hxhim::get(&hx, (void *)kv.first.c_str(), kv.first.size());
    }

    hxhim::flush(&hx);
    MPI_Barrier(MPI_COMM_WORLD);

    hxhim::close(&hx);
    MPI_Finalize();

    return 0;
}
