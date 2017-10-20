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
    char sbuf[256] = {0};
    for (int i = 0; i < 256; i++) {
        sbuf[i] = (char) rand();
    }

    // Send a PUT message to this process
    server.AddPutRequest(sbuf, 128, sbuf + 128, 128);
    server.Flush();

    // Receive the message to this process
    MPIEndpoint localServer;
    size_t nbytes = localServer.PollForMessage(1);
    EXPECT_GT(nbytes, 256);
    // Receive the actual buffer and validate contents
    CommMessage::Type request = CommMessage::INVALID;
    size_t ksize = 0;
    size_t vsize = 0;
    void *kbytes = malloc(128);
    void *vbytes = malloc(128);
    int rc = server.ReceiveRequest(nbytes, &request, &kbytes, &ksize, &vbytes, &vsize);
    EXPECT_EQ(request, CommMessage::PUT);
    EXPECT_EQ(ksize, 128);
    EXPECT_EQ(vsize, 128);
    EXPECT_EQ(0, memcmp(sbuf, kbytes, 128));
    EXPECT_EQ(0, memcmp(sbuf + 128, vbytes, 128));
}

TEST(MPIEndpointTest, GetMessageTest) {
    // Send a GET message to this process
    MPIEndpoint server(0);
    char sbytes[256], rbytes[256] = {0};
    for (int i = 0; i < 256; i++) {
        sbytes[i] = (char) rand();
    }

    // Send a Get message to this process
    server.AddGetRequest(sbytes, 128, sbytes + 128, 128);
    server.Flush();

    // Receive the message to this process
    MPIEndpoint localServer;
    size_t nbytes = localServer.PollForMessage(1);

    // Fill the requested buffer and reply
    EXPECT_GT(nbytes, 256);

    // Receive the actual buffer and validate contents
    //int rc = server.ReceiveMessage(rbytes, nbytes);
    //rc = server.ReplyToMessage();
    //EXPECT_EQ(0, memcmp(sbytes, rbytes, 256));
}

TEST(MPIEndpointTest, PollForMessageTest) {
    // Poll until a message arrives
}

TEST(MPIEndpointTest, WaitForMessageTest) {
    // Wait for a message
}