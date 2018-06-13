//
// Created by bws on 8/24/17.
//

#include "gtest/gtest.h"

#include "transport_mpi.hpp"
#include "MemoryManagers.hpp"

static const char *KEY = "key";
static const std::size_t KEY_LEN = strlen(KEY);
static const char *VALUE = "value";
static const std::size_t VALUE_LEN = strlen(VALUE);

static const std::size_t ALLOC_SIZE = 192;
static const std::size_t REGIONS = 256;

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
    (void)shutdown;
    const MPIInstance& instance = MPIInstance::instance();
    TransportPutMessage src;
    {
        src.mtype = TransportMessageType::PUT;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.rs_idx = 1;

        src.key = &KEY;
        src.key_len = KEY_LEN;

        src.value = &VALUE;
        src.value_len = VALUE_LEN;
    }

    FixedBufferPool *fbp = Memory::Pool(ALLOC_SIZE, REGIONS);
    void *buf = nullptr;
    std::size_t bufsize;
    ASSERT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize, fbp), MDHIM_SUCCESS);

    TransportPutMessage *dst = nullptr;
    ASSERT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    fbp->release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.rs_idx, dst->rs_idx);

    EXPECT_EQ(src.key_len, dst->key_len);
    EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);

    EXPECT_EQ(src.value_len, dst->value_len);
    EXPECT_EQ(memcmp(src.value, dst->value, dst->value_len), 0);

    delete dst;
    EXPECT_EQ(fbp->used(), 0);
}

TEST(mpi_pack_unpack, TransportBPutMessage) {
    volatile int shutdown = 0;
    (void)shutdown;
    const MPIInstance &instance = MPIInstance::instance();
    TransportBPutMessage src;
    {
        src.mtype = TransportMessageType::BPUT;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.num_keys = 1;

        ASSERT_NE(src.rs_idx = new int[src.num_keys](), nullptr);
        src.rs_idx[0] = 1;

        ASSERT_NE(src.keys = new void *[src.num_keys](), nullptr);
        src.keys[0] = (void *) &KEY;

        ASSERT_NE(src.key_lens = new std::size_t[src.num_keys](), nullptr);
        src.key_lens[0] = KEY_LEN;

        ASSERT_NE(src.values = new void *[src.num_keys](), nullptr);
        src.values[0] = (void *) &VALUE;

        ASSERT_NE(src.value_lens = new std::size_t[src.num_keys](), nullptr);
        src.value_lens[0] = VALUE_LEN;
    }

    FixedBufferPool *fbp = Memory::Pool(ALLOC_SIZE, REGIONS);
    void *buf = nullptr;
    std::size_t bufsize;
    ASSERT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize, fbp), MDHIM_SUCCESS);

    TransportBPutMessage *dst = nullptr;
    ASSERT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    fbp->release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);
    EXPECT_EQ(src.rs_idx[0], dst->rs_idx[0]);

    for(std::size_t i = 0; i < dst->num_keys; i++) {
        EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
        EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);

        EXPECT_EQ(src.value_lens[i], dst->value_lens[i]);
        EXPECT_EQ(memcmp(src.values[i], dst->values[i], dst->value_lens[i]), 0);
    }

    delete dst;
    EXPECT_EQ(fbp->used(), 0);
}

TEST(mpi_pack_unpack, TransportGetMessage) {
    volatile int shutdown = 0;
    (void)shutdown;
    const MPIInstance &instance = MPIInstance::instance();
    TransportGetMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.op = TransportGetMessageOp::GET_EQ;
        src.num_keys = 1;
        src.rs_idx = 1;

        src.key = &KEY;
        src.key_len = KEY_LEN;
    }

    FixedBufferPool *fbp = Memory::Pool(ALLOC_SIZE, REGIONS);
    void *buf = nullptr;
    std::size_t bufsize;
    ASSERT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize, fbp), MDHIM_SUCCESS);

    TransportGetMessage *dst = nullptr;
    ASSERT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    fbp->release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);
    EXPECT_EQ(src.rs_idx, dst->rs_idx);

    EXPECT_EQ(src.key_len, dst->key_len);
    EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);

    // normally, the key is transferred over to the response message for deletion
    ::operator delete(dst->key);
    delete dst;
    EXPECT_EQ(fbp->used(), 0);
}

TEST(mpi_pack_unpack, TransportBGetMessage) {
    volatile int shutdown = 0;
    (void)shutdown;
    const MPIInstance &instance = MPIInstance::instance();
    TransportBGetMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.op = TransportGetMessageOp::GET_EQ;
        src.num_keys = 1;

        ASSERT_NE(src.rs_idx = new int[src.num_keys](), nullptr);
        src.rs_idx[0] = 1;

        ASSERT_NE(src.keys = new void *[src.num_keys](), nullptr);
        src.keys[0] = (void *) &KEY;

        ASSERT_NE(src.key_lens = new std::size_t[src.num_keys](), nullptr);
        src.key_lens[0] = KEY_LEN;

        src.num_recs = 1;
    }

    FixedBufferPool *fbp = Memory::Pool(ALLOC_SIZE, REGIONS);
    void *buf = nullptr;
    std::size_t bufsize;
    ASSERT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize, fbp), MDHIM_SUCCESS);

    TransportBGetMessage *dst = nullptr;
    ASSERT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    fbp->release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);
    EXPECT_EQ(src.rs_idx[0], dst->rs_idx[0]);

    for(std::size_t i = 0; i < dst->num_keys; i++) {
        EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
        EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);
    }
    EXPECT_EQ(src.num_recs, dst->num_recs);

    // normally, the keys are transferred over to the response message for deletion
    ::operator delete(dst->keys[0]);
    delete dst;
    EXPECT_EQ(fbp->used(), 0);
}

TEST(mpi_pack_unpack, TransportDeleteMessage) {
    volatile int shutdown = 0;
    (void)shutdown;
    const MPIInstance &instance = MPIInstance::instance();
    TransportDeleteMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.rs_idx = 1;

        src.key = &KEY;
        src.key_len = KEY_LEN;
    }

    FixedBufferPool *fbp = Memory::Pool(ALLOC_SIZE, REGIONS);
    void *buf = nullptr;
    std::size_t bufsize;
    ASSERT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize, fbp), MDHIM_SUCCESS);

    TransportDeleteMessage *dst = nullptr;
    ASSERT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    fbp->release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.rs_idx, dst->rs_idx);

    EXPECT_EQ(src.key_len, dst->key_len);
    EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);

    delete dst;
    EXPECT_EQ(fbp->used(), 0);
}

TEST(mpi_pack_unpack, TransportBDeleteMessage) {
    volatile int shutdown = 0;
    (void)shutdown;
    const MPIInstance &instance = MPIInstance::instance();
    TransportBDeleteMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.num_keys = 1;

        ASSERT_NE(src.rs_idx = new int[src.num_keys](), nullptr);
        src.rs_idx[0] = 1;

        ASSERT_NE(src.keys = new void *[src.num_keys](), nullptr);
        src.keys[0] = (void *) &KEY;

        ASSERT_NE(src.key_lens = new std::size_t[src.num_keys](), nullptr);
        src.key_lens[0] = KEY_LEN;
    }

    FixedBufferPool *fbp = Memory::Pool(ALLOC_SIZE, REGIONS);
    void *buf = nullptr;
    std::size_t bufsize;
    ASSERT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize, fbp), MDHIM_SUCCESS);

    TransportBDeleteMessage *dst = nullptr;
    ASSERT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    fbp->release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);
    EXPECT_EQ(src.rs_idx[0], dst->rs_idx[0]);

    for(std::size_t i = 0; i < dst->num_keys; i++) {
        EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
        EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);
    }

    delete dst;
    EXPECT_EQ(fbp->used(), 0);
}

TEST(mpi_pack_unpack, TransportRecvMessage) {
    volatile int shutdown = 0;
    (void)shutdown;
    const MPIInstance &instance = MPIInstance::instance();
    TransportRecvMessage src;
    {
        src.mtype = TransportMessageType::PUT;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.rs_idx = 1;
        src.error = MDHIM_SUCCESS;
    }

    FixedBufferPool *fbp = Memory::Pool(ALLOC_SIZE, REGIONS);
    void *buf = nullptr;
    std::size_t bufsize;
    ASSERT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize, fbp), MDHIM_SUCCESS);

    TransportRecvMessage *dst = nullptr;
    ASSERT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    fbp->release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.rs_idx, dst->rs_idx);
    EXPECT_EQ(src.error, dst->error);

    delete dst;
    EXPECT_EQ(fbp->used(), 0);
}

TEST(mpi_pack_unpack, TransportGetRecvMessage) {
    volatile int shutdown = 0;
    (void)shutdown;
    const MPIInstance &instance = MPIInstance::instance();
    TransportGetRecvMessage src;
    {
        src.mtype = TransportMessageType::RECV_BGET;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.error = MDHIM_SUCCESS;

        src.rs_idx = 1;

        src.key = (void *) &KEY;
        src.key_len = KEY_LEN;

        // malloc because value comes from database
        src.value = malloc(VALUE_LEN);
        memcpy(src.value, VALUE, VALUE_LEN * sizeof(char));

        src.value_len = VALUE_LEN;
    }

    FixedBufferPool *fbp = Memory::Pool(ALLOC_SIZE, REGIONS);
    void *buf = nullptr;
    std::size_t bufsize;
    ASSERT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize, fbp), MDHIM_SUCCESS);

    TransportGetRecvMessage *dst = nullptr;
    ASSERT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    fbp->release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.error, dst->error);
    EXPECT_EQ(src.rs_idx, dst->rs_idx);

    EXPECT_EQ(src.key_len, dst->key_len);
    EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);

    EXPECT_EQ(src.value_len, dst->value_len);
    EXPECT_EQ(memcmp(src.value, dst->value, dst->value_len), 0);

    delete dst;
    EXPECT_EQ(fbp->used(), 0);
}

TEST(mpi_pack_unpack, TransportBGetRecvMessage) {
    volatile int shutdown = 0;
    (void)shutdown;
    const MPIInstance &instance = MPIInstance::instance();
    TransportBGetRecvMessage src;
    {
        src.mtype = TransportMessageType::RECV_BGET;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.error = MDHIM_SUCCESS;

        src.num_keys = 1;

        ASSERT_NE(src.rs_idx = new int[src.num_keys](), nullptr);
        src.rs_idx[0] = 1;

        // the key comes from the database
        ASSERT_NE(src.keys = new void *[src.num_keys](), nullptr);
        src.keys[0] = calloc(KEY_LEN, sizeof(char));
        memcpy(src.keys[0], (void *) &KEY, KEY_LEN);

        ASSERT_NE(src.key_lens = new std::size_t[src.num_keys](), nullptr);
        src.key_lens[0] = KEY_LEN;

        ASSERT_NE(src.values = new void *[src.num_keys](), nullptr);
        src.values[0] = malloc(VALUE_LEN);
        memcpy(src.values[0], VALUE, VALUE_LEN * sizeof(char));

        ASSERT_NE(src.value_lens = new std::size_t[src.num_keys](), nullptr);
        src.value_lens[0] = VALUE_LEN;

        src.next = nullptr;
    }

    FixedBufferPool *fbp = Memory::Pool(ALLOC_SIZE, REGIONS);
    void *buf = nullptr;
    std::size_t bufsize;
    ASSERT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize, fbp), MDHIM_SUCCESS);

    TransportBGetRecvMessage *dst = nullptr;
    ASSERT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    fbp->release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.error, dst->error);
    EXPECT_EQ(src.num_keys, dst->num_keys);
    EXPECT_EQ(src.rs_idx[0], dst->rs_idx[0]);

    for(std::size_t i = 0; i < dst->num_keys; i++) {
        EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
        EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);

        EXPECT_EQ(src.value_lens[i], dst->value_lens[i]);
        EXPECT_EQ(memcmp(src.values[i], dst->values[i], dst->value_lens[i]), 0);
    }

    delete dst;
    EXPECT_EQ(fbp->used(), 0);
}

TEST(mpi_pack_unpack, TransportBRecvMessage) {
    volatile int shutdown = 0;
    (void)shutdown;
    const MPIInstance &instance = MPIInstance::instance();
    TransportBRecvMessage src;
    {
        src.mtype = TransportMessageType::RECV_BGET;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.error = MDHIM_SUCCESS;

        src.num_keys = 1;

        ASSERT_NE(src.rs_idx = new int[src.num_keys](), nullptr);
        src.rs_idx[0] = 1;

        src.next = nullptr;
    }

    FixedBufferPool *fbp = Memory::Pool(ALLOC_SIZE, REGIONS);
    void *buf = nullptr;
    std::size_t bufsize;
    ASSERT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize, fbp), MDHIM_SUCCESS);

    TransportBRecvMessage *dst = nullptr;
    ASSERT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    fbp->release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.error, dst->error);
    EXPECT_EQ(src.num_keys, dst->num_keys);
    EXPECT_EQ(src.rs_idx[0], dst->rs_idx[0]);

    delete dst;
    EXPECT_EQ(fbp->used(), 0);
}
