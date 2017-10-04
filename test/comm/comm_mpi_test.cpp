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
        //int mpiError = MPI_Init(&argc, &argv);
        //ASSERT_FALSE(mpiError);
    }
    virtual void TearDown() {
        //int mpiError = MPI_Finalize();
        //ASSERT_FALSE(mpiError);
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

// Test MPIAddress()
TEST(MPIAddressTest, DefaultConstructorTest) {

    const MPIAddress addr0(0);
    EXPECT_EQ(0, addr0.Rank());
    const MPIAddress addr1(1);
    EXPECT_EQ(1, addr1.Rank());
}

// Test MPIEndpoint()
TEST(MPIEndpointTest, DefaultConstructorTest) {
    MPIEndpoint ep1, ep2;

}

TEST(MPIEndpointTest, PutMessageTest) {
    MPIEndpoint server(0);
    char sbytes[256], rbytes[256] = {0};
    for (int i = 0; i < 256; i++) {
        sbytes[i] = (char) rand();
    }

    // Send a PUT message to this process
    server.PutMessage(sbytes, 256);
    server.Flush();

    // Receive the message to this process
    MPIEndpoint localServer;
    size_t nbytes = localServer.PollForMessage(1);

    // Ensure the received buffer is the same as the sent buffer
    EXPECT_EQ(256, nbytes);

    // Receive the actual buffer and validate contents
    int rc = server.ReceiveMessage(rbytes, nbytes);
    EXPECT_EQ(0, memcmp(sbytes, rbytes, 256));
}

TEST(MPIEndpointTest, GetMessageTest) {
    // Send a GET message to this process
}

TEST(MPIEndpointTest, PollForMessageTest) {
    // Poll until a message arrives
}

TEST(MPIEndpointTest, WaitForMessageTest) {
    // Wait for a message
}