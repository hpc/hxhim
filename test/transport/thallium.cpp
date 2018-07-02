#include <sstream>
#include <unistd.h>

#include "gtest/gtest.h"

#include "transport/Thallium.hpp"

static const char *KEY = "key";
static const std::size_t KEY_LEN = strlen(KEY);
static const char *VALUE = "value";
static const std::size_t VALUE_LEN = strlen(VALUE);

TEST(thallium_pack_unpack, TransportPutMessage) {
    TransportPutMessage src;
    {
        src.mtype = TransportMessageType::PUT;
        src.src = 1;
        src.dst = 1;
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.rs_idx = 1;

        src.key = &KEY;
        src.key_len = KEY_LEN;

        src.value = &VALUE;
        src.value_len = VALUE_LEN;
    }

    std::string buf;
    EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

    TransportPutMessage *dst = nullptr;
    EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

    ASSERT_NE(dst, nullptr);
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
}

TEST(thallium_pack_unpack, TransportBPutMessage) {
    TransportBPutMessage src;
    {
        src.mtype = TransportMessageType::BPUT;
        src.src = 1;
        src.dst = 1;
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

    std::string buf;
    EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

    TransportBPutMessage *dst = nullptr;
    EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

    ASSERT_NE(dst, nullptr);
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
}

TEST(thallium_pack_unpack, TransportGetMessage) {
    TransportGetMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.src = 1;
        src.dst = 1;
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.op = TransportGetMessageOp::GET_EQ;
        src.num_keys = 1;

        src.rs_idx = 1;

        src.key = &KEY;
        src.key_len = KEY_LEN;
    }

    std::string buf;
    EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

    TransportGetMessage *dst = nullptr;
    EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);
    EXPECT_EQ(src.key_len, dst->key_len);
    EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);

    // normally, the key is transferred over to the response message for deletion
    ::operator delete(dst->key);
    delete dst;
}

TEST(thallium_pack_unpack, TransportBGetMessage) {
    TransportBGetMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.src = 1;
        src.dst = 1;
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

    std::string buf;
    EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

    TransportBGetMessage *dst = nullptr;
    EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

    ASSERT_NE(dst, nullptr);
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
}

TEST(thallium_pack_unpack, TransportDeleteMessage) {
    TransportDeleteMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.src = 1;
        src.dst = 1;
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.key = &KEY;
        src.key_len = KEY_LEN;
    }

    std::string buf;
    EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

    TransportDeleteMessage *dst = nullptr;
    EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.key_len, dst->key_len);
    EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);

    delete dst;
}

TEST(thallium_pack_unpack, TransportBDeleteMessage) {
    TransportBDeleteMessage src;
    {
        src.mtype = TransportMessageType::BGET;
        src.src = 1;
        src.dst = 1;
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

    std::string buf;
    EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

    TransportBDeleteMessage *dst = nullptr;
    EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

    ASSERT_NE(dst, nullptr);
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
}

TEST(thallium_pack_unpack, TransportRecvMessage) {
    TransportRecvMessage src;
    {
        src.mtype = TransportMessageType::PUT;
        src.src = 1;
        src.dst = 1;
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.error = MDHIM_SUCCESS;
    }

    std::string buf;
    EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

    TransportRecvMessage *dst = nullptr;
    EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.error, dst->error);

    delete dst;
}

TEST(thallium_pack_unpack, TransportBGetRecvMessage) {
    TransportBGetRecvMessage src;
    {
        src.mtype = TransportMessageType::RECV_BGET;
        src.src = 1;
        src.dst = 1;
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

    std::string buf;
    EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

    TransportBGetRecvMessage *dst = nullptr;
    EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

    ASSERT_NE(dst, nullptr);
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
}

TEST(thallium_pack_unpack, TransportBRecvMessage) {
    TransportBRecvMessage src;
    {
        src.mtype = TransportMessageType::RECV_BULK;
        src.src = 1;
        src.dst = 1;
        src.index = 1;
        src.index_type = PRIMARY_INDEX;
        src.index_name = nullptr;

        src.error = MDHIM_SUCCESS;
        src.num_keys = 1;

        ASSERT_NE(src.rs_idx = new int[src.num_keys](), nullptr);
        src.rs_idx[0] = 1;

        src.next = nullptr;
    }

    std::string buf;
    EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

    TransportBRecvMessage *dst = nullptr;
    EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.mtype, dst->mtype);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);
    EXPECT_EQ(src.index, dst->index);
    EXPECT_EQ(src.index_type, dst->index_type);

    EXPECT_EQ(src.num_keys, dst->num_keys);
    EXPECT_EQ(src.rs_idx[0], dst->rs_idx[0]);

    delete dst;
}
