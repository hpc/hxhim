#include <cstdlib>
#include <ctime>

#include <gtest/gtest.h>
#include <mpi.h>

int main(int argc, char **argv) {
    srand(time(NULL));

    ::testing::InitGoogleTest(&argc, argv);

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    const int rc = RUN_ALL_TESTS();
    MPI_Finalize();

    return rc;
}
