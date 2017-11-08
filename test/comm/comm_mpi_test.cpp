//
// Created by bws on 8/24/17.
//

#include "gtest/gtest.h"
#include "comm_mpi.h"

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

    // Initialize some data into a k-v pair
    char sbuf[256] = {0};
    for (int i = 0; i < 256; i++) {
        sbuf[i] = (char) rand();
    }
    char* key = sbuf;
    char* value = sbuf + 128;

    // Send a PUT message to this process
    server.AddPutRequest(key, 128, value, 128);
    server.Flush();

    // Receive the message to this process
    MPIEndpoint localServer;
    size_t nbytes = localServer.PollForMessage(1);
    EXPECT_GT(nbytes, 256);

    // Parse the actual buffer and validate contents
    CommMessage::Type request = CommMessage::INVALID;
    size_t ksize = 0;
    size_t vsize = 0;
    void *kbuf = malloc(128);
    void *vbuf = malloc(128);
    int rc = server.ReceiveRequest(nbytes, &request, &kbuf, &ksize, &vbuf, &vsize);
    EXPECT_EQ(request, CommMessage::PUT);
    EXPECT_EQ(ksize, 128);
    EXPECT_EQ(vsize, 128);
    EXPECT_EQ(0, memcmp(sbuf, kbuf, 128));
    EXPECT_EQ(0, memcmp(sbuf + 128, vbuf, 128));
    free(kbuf);
    free(vbuf);
}

TEST(MPIEndpointTest, GetMessageTest) {
    MPIEndpoint server(0);

    // Initialize some data into a k-v pair Key
    char sbuf[128] = {0};
    for (int i = 0; i < 128; i++) {
        sbuf[i] = (char) rand();
    }
    char* key = sbuf;
    char value[128] = {0};

    // Send a PUT message to this process
    server.AddGetRequest(key, 128, value, 128);
    server.Flush();

    // Receive the message to this process
    MPIEndpoint localServer;
    size_t nbytes = localServer.PollForMessage(1);
    EXPECT_GT(nbytes, 128);

    // Parse the actual buffer and validate contents
    CommMessage::Type request = CommMessage::INVALID;
    size_t ksize = 0;
    size_t vsize = 0;
    void *kbuf = malloc(128);
    void *vbuf = malloc(128);
    int rc = server.ReceiveRequest(nbytes, &request, &kbuf, &ksize, &vbuf, &vsize);
    EXPECT_EQ(request, CommMessage::GET);
    EXPECT_EQ(ksize, 128);
    EXPECT_EQ(vsize, 0);
    EXPECT_EQ(0, memcmp(sbuf, kbuf, 128));
    EXPECT_EQ(0, memcmp(sbuf + 128, vbuf, vsize));
    free(kbuf);
    free(vbuf);
}

TEST(MPIEndpointTest, PollForMessageTest) {
    // Poll until a message arrives
}

TEST(MPIEndpointTest, WaitForMessageTest) {
    // Wait for a message
}

TEST(MPIInstanceTest, RankTest) {
    const MPIInstance& instance = MPIInstance::instance();
    EXPECT_EQ(instance.Rank(), 0);
}

TEST(MPIInstanceTest, SizeTest) {
    const MPIInstance& instance = MPIInstance::instance();
    EXPECT_EQ(instance.Size(), 1);
}

TEST(MPIInstanceTest, WorldRankTest) {
    const MPIInstance& instance = MPIInstance::instance();
    EXPECT_EQ(instance.WorldRank(), 0);
}

TEST(MPIInstanceTest, WorldSizeTest) {
    const MPIInstance& instance = MPIInstance::instance();
    EXPECT_EQ(instance.WorldSize(), 1);
}

