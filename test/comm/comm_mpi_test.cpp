//
// Created by bws on 8/24/17.
//

#include "gtest/gtest.h"
#include "comm_mpi.h"

/**
 * Setup a custom MPI environment for this test
 */
class MPIEnvironment : public ::testing::Environment
{
public:
    virtual void SetUp() {
        char** argv;
        int argc = 0;
        int mpiError = MPI_Init(&argc, &argv);
        ASSERT_FALSE(mpiError);
    }
    virtual void TearDown() {
        int mpiError = MPI_Finalize();
        ASSERT_FALSE(mpiError);
    }
    virtual ~MPIEnvironment() {}
};

/**
 * Setup a custom main() for this particular unit test.  This isn't typically needed, but for environment
 * bootstrapping is required.
 *
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char* argv[])
{
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::AddGlobalTestEnvironment(new MPIEnvironment);
    return RUN_ALL_TESTS();
}

// Test Endpoint
TEST(MPIAddress, DefaultConstructor) {

    const MPIAddress addr(0);
    EXPECT_EQ(0, addr.Rank());
}