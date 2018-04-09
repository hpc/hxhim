//
// Created by bws on 8/24/17.
//

#include "gtest/gtest.h"

#include "transport_mpi.hpp"
#include "MemoryManagers.hpp"

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
    TransportPutMessage src;
    {
        src.mtype = TransportMessageType::PUT;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.key = ::operator new(KEY_LEN);
        memcpy(src.key, KEY, KEY_LEN * sizeof(char));
        src.key_len = KEY_LEN;

        src.value = ::operator new(VALUE_LEN);
        memcpy(src.value, VALUE, VALUE_LEN * sizeof(char));
        src.value_len = VALUE_LEN;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportPutMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    Memory::FBP_MEDIUM::Instance().release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.key_len, dst->key_len);
    EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);

    EXPECT_EQ(src.value_len, dst->value_len);
    EXPECT_EQ(memcmp(src.value, dst->value, dst->value_len), 0);

    delete dst;
    EXPECT_EQ(Memory::FBP_MEDIUM::Instance().used(), 0);
}

TEST(mpi_pack_unpack, TransportBPutMessage) {
    volatile int shutdown = 0;
    const MPIInstance &instance = MPIInstance::instance();
    TransportBPutMessage src;
    {
        src.mtype = TransportMessageType::BPUT;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.keys = new void *[1]();
        src.keys[0] = ::operator new(KEY_LEN);
        memcpy(src.keys[0], KEY, KEY_LEN * sizeof(char));

        src.key_lens = new int[1]();;
        src.key_lens[0] = KEY_LEN;

        src.values = new void *[1]();
        src.values[0] = (void *)::operator new(VALUE_LEN);
        memcpy(src.values[0], VALUE, VALUE_LEN * sizeof(char));

        src.value_lens = new int[1]();;
        src.value_lens[0] = VALUE_LEN;

        src.num_keys = 1;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportBPutMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    Memory::FBP_MEDIUM::Instance().release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);

    for(int i = 0; i < dst->num_keys; i++) {
        EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
        EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);

        EXPECT_EQ(src.value_lens[i], dst->value_lens[i]);
        EXPECT_EQ(memcmp(src.values[i], dst->values[i], dst->value_lens[i]), 0);
    }

    delete dst;
    EXPECT_EQ(Memory::FBP_MEDIUM::Instance().used(), 0);
}

TEST(mpi_pack_unpack, TransportGetMessage) {
    volatile int shutdown = 0;
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

        src.key = ::operator new(KEY_LEN);
        memcpy(src.key, KEY, KEY_LEN * sizeof(char));

        src.key_len = KEY_LEN;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportGetMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    Memory::FBP_MEDIUM::Instance().release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);
    EXPECT_EQ(src.key_len, dst->key_len);
    EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);

    delete dst;
    EXPECT_EQ(Memory::FBP_MEDIUM::Instance().used(), 0);
}

TEST(mpi_pack_unpack, TransportBGetMessage) {
    volatile int shutdown = 0;
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

        src.keys = new void *[1]();
        src.keys[0] = ::operator new(KEY_LEN);
        memcpy(src.keys[0], KEY, KEY_LEN * sizeof(char));

        src.key_lens = new int[1]();;
        src.key_lens[0] = KEY_LEN;

        src.num_recs = 1;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportBGetMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    Memory::FBP_MEDIUM::Instance().release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);

    for(int i = 0; i < dst->num_keys; i++) {
        EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
        EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);
    }
    EXPECT_EQ(src.num_recs, dst->num_recs);

    delete dst;
    EXPECT_EQ(Memory::FBP_MEDIUM::Instance().used(), 0);
}

TEST(mpi_pack_unpack, TransportDeleteMessage) {
    volatile int shutdown = 0;
    const MPIInstance &instance = MPIInstance::instance();
    TransportDeleteMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.key = ::operator new(KEY_LEN);
        memcpy(src.key, KEY, KEY_LEN * sizeof(char));

        src.key_len = KEY_LEN;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportDeleteMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    Memory::FBP_MEDIUM::Instance().release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.key_len, dst->key_len);
    EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);

    delete dst;
    EXPECT_EQ(Memory::FBP_MEDIUM::Instance().used(), 0);
}

TEST(mpi_pack_unpack, TransportBDeleteMessage) {
    volatile int shutdown = 0;
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

        src.keys = new void *[1]();
        src.keys[0] = ::operator new(KEY_LEN);
        memcpy(src.keys[0], KEY, KEY_LEN * sizeof(char));

        src.key_lens = new int[1]();;
        src.key_lens[0] = KEY_LEN;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportBDeleteMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    Memory::FBP_MEDIUM::Instance().release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);

    for(int i = 0; i < dst->num_keys; i++) {
        EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
        EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);
    }

    delete dst;
    EXPECT_EQ(Memory::FBP_MEDIUM::Instance().used(), 0);
}

TEST(mpi_pack_unpack, TransportRecvMessage) {
    volatile int shutdown = 0;
    const MPIInstance &instance = MPIInstance::instance();
    TransportRecvMessage src;
    {
        src.mtype = TransportMessageType::PUT;
        src.src = instance.Rank();
        src.dst = instance.Rank();
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.error = MDHIM_SUCCESS;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportRecvMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    Memory::FBP_MEDIUM::Instance().release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.error, dst->error);

    delete dst;
    EXPECT_EQ(Memory::FBP_MEDIUM::Instance().used(), 0);
}

TEST(mpi_pack_unpack, TransportBGetRecvMessage) {
    volatile int shutdown = 0;
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

        src.keys = new void *[1]();
        src.keys[0] = ::operator new(KEY_LEN);
        memcpy(src.keys[0], KEY, KEY_LEN * sizeof(char));

        src.key_lens = new int[1]();;
        src.key_lens[0] = KEY_LEN;

        src.values = new void *[1]();
        src.values[0] = (void *)::operator new(VALUE_LEN);
        memcpy(src.values[0], VALUE, VALUE_LEN * sizeof(char));

        src.value_lens = new int[1]();;
        src.value_lens[0] = VALUE_LEN;

        src.num_keys = 1;
        src.next = nullptr;
    }

    void *buf = nullptr;
    int bufsize;
    EXPECT_EQ(MPIPacker::pack(instance.Comm(), &src, &buf, &bufsize), MDHIM_SUCCESS);

    TransportBGetRecvMessage *dst = nullptr;
    EXPECT_EQ(MPIUnpacker::unpack(instance.Comm(), &dst, buf, bufsize), MDHIM_SUCCESS);

    Memory::FBP_MEDIUM::Instance().release(buf);

    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.error, dst->error);

    EXPECT_EQ(src.num_keys, dst->num_keys);

    for(int i = 0; i < dst->num_keys; i++) {
        EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
        EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);

        EXPECT_EQ(src.value_lens[i], dst->value_lens[i]);
        EXPECT_EQ(memcmp(src.values[i], dst->values[i], dst->value_lens[i]), 0);
    }

    delete dst;
    EXPECT_EQ(Memory::FBP_MEDIUM::Instance().used(), 0);
}
