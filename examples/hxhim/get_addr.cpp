#include <map>

#include <mpi.h>

#include "transport/backend/Thallium/Utilities.hpp"

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    thallium::engine engine("tcp", THALLIUM_SERVER_MODE, true, -1);
    std::map<int, std::string> addrs;
    Transport::Thallium::get_addrs(MPI_COMM_WORLD, engine, addrs);

    MPI_Barrier(MPI_COMM_WORLD);

    engine.finalize();
    MPI_Finalize();

    return 0;
}
