//
// Created by bws on 8/24/17.
//

#include "gtest/gtest.h"

#include "indexes.h"
#include "transport_mpi.hpp"

static const char *KEY = "key";
static const int KEY_LEN = strlen(KEY);
static const char *VALUE = "value";
static const int VALUE_LEN = strlen(VALUE);

TEST(MPIInstance, Rank) {
    const MPIInstance& instance = MPIInstance::instance();
    EXPECT_EQ(instance.Rank(), 0);
}

TEST(MPIInstance, Size) {
    const MPIInstance& instance = MPIInstance::instance();
    EXPECT_EQ(instance.Size(), 1);
}

TEST(MPIInstance, WorldRank) {
    const MPIInstance& instance = MPIInstance::instance();
    EXPECT_EQ(instance.WorldRank(), 0);
}

TEST(MPIInstance, WorldSize) {
    const MPIInstance& instance = MPIInstance::instance();
    EXPECT_EQ(instance.WorldSize(), 1);
}

TEST(mpi_pack_unpack, TransportPutMessage) {
    volatile int shutdown = 0;
    const MPIInstance &instance = MPIInstance::instance();
    MPITransportBase commbase(instance.Comm(), shutdown);
    TransportPutMessage src;
    {
        src.mtype = TransportMessageType::PUT;
        src.server_rank = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.key = malloc(KEY_LEN * sizeof(char));
        memcpy(src.key, KEY, KEY_LEN * sizeof(char));
        src.key_len = KEY_LEN;

        src.value = malloc(VALUE_LEN * sizeof(char));
        memcpy(src.value, VALUE, VALUE_LEN * sizeof(char));
        src.value_len = VALUE_LEN;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(&commbase, &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportPutMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(&commbase, &dst, buf, bufsize), MDHIM_SUCCESS);

    free(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.server_rank, dst->server_rank);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.key_len, dst->key_len);
    EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);

    EXPECT_EQ(src.value_len, dst->value_len);
    EXPECT_EQ(memcmp(src.value, dst->value, dst->value_len), 0);
}

TEST(mpi_pack_unpack, TransportBPutMessage) {
    volatile int shutdown = 0;
    const MPIInstance &instance = MPIInstance::instance();
    MPITransportBase commbase(instance.Comm(), shutdown);
    TransportBPutMessage src;
    {
        src.mtype = TransportMessageType::BPUT;
        src.server_rank = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.keys = (void **)malloc(sizeof(void *));
        src.keys[0] = (void *)malloc(KEY_LEN * sizeof(char));
        memcpy(src.keys[0], KEY, KEY_LEN * sizeof(char));

        src.key_lens = (int *)malloc(sizeof(int));
        src.key_lens[0] = KEY_LEN;

        src.values = (void **)malloc(sizeof(void *));
        src.values[0] = (void *)malloc(VALUE_LEN * sizeof(char));
        memcpy(src.values[0], VALUE, VALUE_LEN * sizeof(char));

        src.value_lens = (int *)malloc(sizeof(int));
        src.value_lens[0] = VALUE_LEN;

        src.num_keys = 1;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(&commbase, &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportBPutMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(&commbase, &dst, buf, bufsize), MDHIM_SUCCESS);

    free(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.server_rank, dst->server_rank);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);

    for(int i = 0; i < dst->num_keys; i++) {
        EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
        EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);

        EXPECT_EQ(src.value_lens[i], dst->value_lens[i]);
        EXPECT_EQ(memcmp(src.values[i], dst->values[i], dst->value_lens[i]), 0);
    }
}

TEST(mpi_pack_unpack, TransportGetMessage) {
    volatile int shutdown = 0;
    const MPIInstance &instance = MPIInstance::instance();
    MPITransportBase commbase(instance.Comm(), shutdown);
    TransportGetMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.server_rank = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.op = TransportGetMessageOp::GET_EQ;
        src.num_keys = 1;

        src.key = (void *)malloc(KEY_LEN * sizeof(char));
        memcpy(src.key, KEY, KEY_LEN * sizeof(char));

        src.key_len = KEY_LEN;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(&commbase, &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportGetMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(&commbase, &dst, buf, bufsize), MDHIM_SUCCESS);

    free(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.server_rank, dst->server_rank);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);
    EXPECT_EQ(src.key_len, dst->key_len);
    EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);
}

TEST(mpi_pack_unpack, TransportBGetMessage) {
    volatile int shutdown = 0;
    const MPIInstance &instance = MPIInstance::instance();
    MPITransportBase commbase(instance.Comm(), shutdown);
    TransportBGetMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.server_rank = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.op = TransportGetMessageOp::GET_EQ;
        src.num_keys = 1;

        src.keys = (void **)malloc(sizeof(void *));
        src.keys[0] = (void *)malloc(KEY_LEN * sizeof(char));
        memcpy(src.keys[0], KEY, KEY_LEN * sizeof(char));

        src.key_lens = (int *)malloc(sizeof(int));
        src.key_lens[0] = KEY_LEN;

        src.num_recs = 1;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(&commbase, &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportBGetMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(&commbase, &dst, buf, bufsize), MDHIM_SUCCESS);

    free(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.server_rank, dst->server_rank);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);

    for(int i = 0; i < dst->num_keys; i++) {
        EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
        EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);
    }

    EXPECT_EQ(src.num_recs, dst->num_recs);
}

TEST(mpi_pack_unpack, TransportBGetRecvMessage) {
    volatile int shutdown = 0;
    const MPIInstance &instance = MPIInstance::instance();
    MPITransportBase commbase(instance.Comm(), shutdown);
    TransportBGetRecvMessage src;
    {
        src.mtype = TransportMessageType::RECV_BGET;
        src.server_rank = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.error = MDHIM_SUCCESS;

        src.keys = (void **)malloc(sizeof(void *));
        src.keys[0] = (void *)malloc(KEY_LEN * sizeof(char));
        memcpy(src.keys[0], KEY, KEY_LEN * sizeof(char));

        src.key_lens = (int *)malloc(sizeof(int));
        src.key_lens[0] = KEY_LEN;

        src.values = (void **)malloc(sizeof(void *));
        src.values[0] = (void *)malloc(VALUE_LEN * sizeof(char));
        memcpy(src.values[0], VALUE, VALUE_LEN * sizeof(char));

        src.value_lens = (int *)malloc(sizeof(int));
        src.value_lens[0] = VALUE_LEN;

        src.num_keys = 1;
        src.next = nullptr;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(&commbase, &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportBGetRecvMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(&commbase, &dst, buf, bufsize), MDHIM_SUCCESS);

    free(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.server_rank, dst->server_rank);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);

    for(int i = 0; i < dst->num_keys; i++) {
        EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
        EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);

        EXPECT_EQ(src.value_lens[i], dst->value_lens[i]);
        EXPECT_EQ(memcmp(src.values[i], dst->values[i], dst->value_lens[i]), 0);
    }
}

TEST(mpi_pack_unpack, TransportDeleteMessage) {
    volatile int shutdown = 0;
    const MPIInstance &instance = MPIInstance::instance();
    MPITransportBase commbase(instance.Comm(), shutdown);
    TransportDeleteMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.server_rank = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.key = (void *)malloc(KEY_LEN * sizeof(char));
        memcpy(src.key, KEY, KEY_LEN * sizeof(char));

        src.key_len = KEY_LEN;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(&commbase, &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportDeleteMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(&commbase, &dst, buf, bufsize), MDHIM_SUCCESS);

    free(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.server_rank, dst->server_rank);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.key_len, dst->key_len);
    EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);
}

TEST(mpi_pack_unpack, TransportBDeleteMessage) {
    volatile int shutdown = 0;
    const MPIInstance &instance = MPIInstance::instance();
    MPITransportBase commbase(instance.Comm(), shutdown);
    TransportBDeleteMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.server_rank = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.num_keys = 1;

        src.keys = (void **)malloc(sizeof(void *));
        src.keys[0] = (void *)malloc(KEY_LEN * sizeof(char));
        memcpy(src.keys[0], KEY, KEY_LEN * sizeof(char));

        src.key_lens = (int *)malloc(sizeof(int));
        src.key_lens[0] = KEY_LEN;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(&commbase, &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportBDeleteMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(&commbase, &dst, buf, bufsize), MDHIM_SUCCESS);

    free(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.server_rank, dst->server_rank);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);

    for(int i = 0; i < dst->num_keys; i++) {
        EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
        EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);
    }
}

TEST(mpi_pack_unpack, TransportRecvMessage) {
    volatile int shutdown = 0;
    const MPIInstance &instance = MPIInstance::instance();
    MPITransportBase commbase(instance.Comm(), shutdown);
    TransportRecvMessage src;
    {
        src.mtype = TransportMessageType::PUT;
        src.server_rank = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.error = MDHIM_SUCCESS;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(&commbase, &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportRecvMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(&commbase, &dst, buf, bufsize), MDHIM_SUCCESS);

    free(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.server_rank, dst->server_rank);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.error, dst->error);
}

TEST(mpi_pack_unpack, TransportBRecvMessage) {
    volatile int shutdown = 0;
    const MPIInstance &instance = MPIInstance::instance();
    MPITransportBase commbase(instance.Comm(), shutdown);
    TransportBRecvMessage src;
    {
        src.mtype = TransportMessageType::PUT;
        src.server_rank = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.error = MDHIM_SUCCESS;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(&commbase, &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportBRecvMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(&commbase, &dst, buf, bufsize), MDHIM_SUCCESS);

    free(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.server_rank, dst->server_rank);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.error, dst->error);
}

// // Test MPIEndpoint()
// TEST(MPIEndpointTest, DefaultConstructorTest) {
//     MPIEndpoint ep1(MPI_COMM_WORLD), ep2(MPI_COMM_WORLD);
// }

// TEST(MPIEndpointTest, PutMessageTest) {
//     const int rank = 0;
//     MPIEndpoint server(MPI_COMM_WORLD);

//     // Initialize some data into a k-v pair
//     char sbuf[256] = {0};
//     for (int i = 0; i < 256; i++) {
//         sbuf[i] = (char) rand();
//     }
//     char* key = sbuf;
//     char* value = sbuf + 128;

//     // Send a PUT message to this process
//     server.AddPutRequest(rank, key, 128, value, 128);
//     server.Flush();

//     // Receive the message to this process
//     MPIEndpoint localServer(MPI_COMM_WORLD);
//     size_t nbytes = localServer.PollForMessage(1);
//     EXPECT_GT(nbytes, 256);

//     // Parse the actual buffer and validate contents
//     TransportMessageType request = TransportMessage::INVALID;
//     size_t ksize = 0;
//     size_t vsize = 0;
//     void *kbuf = malloc(128);
//     void *vbuf = malloc(128);
//     int rc = server.ReceiveRequest(nbytes, &request, &kbuf, &ksize, &vbuf, &vsize);
//     EXPECT_EQ(request, TransportMessage::PUT);
//     EXPECT_EQ(ksize, 128);
//     EXPECT_EQ(vsize, 128);
//     EXPECT_EQ(0, memcmp(sbuf, kbuf, 128));
//     EXPECT_EQ(0, memcmp(sbuf + 128, vbuf, 128));
//     free(kbuf);
//     free(vbuf);
// }

// TEST(MPIEndpointTest, GetMessageTest) {
//     const int rank = 0;
//     MPIEndpoint server(MPI_COMM_WORLD);

//     // Initialize some data into a k-v pair Key
//     char sbuf[128] = {0};
//     for (int i = 0; i < 128; i++) {
//         sbuf[i] = (char) rand();
//     }
//     char* key = sbuf;
//     char value[128] = {0};

//     // Send a PUT message to this process
//     server.AddGetRequest(rank, key, 128, value, 128);
//     server.Flush();

//     // Receive the message to this process
//     MPIEndpoint localServer(MPI_COMM_WORLD);
//     size_t nbytes = localServer.PollForMessage(1);
//     EXPECT_GT(nbytes, 128);

//     // Parse the actual buffer and validate contents
//     TransportMessageType request = TransportMessage::INVALID;
//     size_t ksize = 0;
//     size_t vsize = 0;
//     void *kbuf = malloc(128);
//     void *vbuf = malloc(128);
//     int rc = server.ReceiveRequest(nbytes, &request, &kbuf, &ksize, &vbuf, &vsize);
//     EXPECT_EQ(request, TransportMessage::GET);
//     EXPECT_EQ(ksize, 128);
//     EXPECT_EQ(vsize, 0);
//     EXPECT_EQ(0, memcmp(sbuf, kbuf, 128));
//     EXPECT_EQ(0, memcmp(sbuf + 128, vbuf, vsize));
//     free(kbuf);
//     free(vbuf);
// }

// TEST(MPIEndpointTest, PollForMessageTest) {
//     // Poll until a message arrives
// }

// TEST(MPIEndpointTest, WaitForMessageTest) {
//     // Wait for a message
// }
