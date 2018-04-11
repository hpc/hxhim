#include <sstream>
#include <unistd.h>

#include "gtest/gtest.h"

#include "transport_thallium.hpp"
#include "MemoryManagers.hpp"

static const char *KEY = "key";
static const int KEY_LEN = strlen(KEY);
static const char *VALUE = "value";
static const int VALUE_LEN = strlen(VALUE);

TEST(thallium_pack_unpack, TransportPutMessage) {
    TransportPutMessage src;
    {
        src.mtype = TransportMessageType::PUT;
        src.src = 1;
        src.dst = 1;
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
    EXPECT_EQ(Memory::MESSAGE_BUFFER::Instance().used(), 0);
}

// TEST(thallium_pack_unpack, TransportBPutMessage) {
//     TransportBPutMessage src;
//     {
//         src.mtype = TransportMessageType::BPUT;
//         src.src = 1;
//         src.dst = 1;
//         src.index = 1;
//         src.index_type = PRIMARY_INDEX;
//         src.index_name = nullptr;

//         src.keys = new void*[1]();;
//         src.keys[0] = ::operator new(KEY_LEN);
//         memcpy(src.keys[0], KEY, KEY_LEN * sizeof(char));

//         src.key_lens = new int[1]();
//         src.key_lens[0] = KEY_LEN;

//         src.values = new void*[1]();;
//         src.values[0] = ::operator new(VALUE_LEN);
//         memcpy(src.values[0], VALUE, VALUE_LEN * sizeof(char));

//         src.value_lens = new int[1]();
//         src.value_lens[0] = VALUE_LEN;

//         src.num_keys = 1;
//     }

//     void *buf = nullptr;
//     int bufsize;
//     EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

//     TransportBPutMessage *dst = nullptr;
//     EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

//     ASSERT_NE(dst, nullptr);
//     EXPECT_EQ(src.mtype, dst->mtype);
//     EXPECT_EQ(src.src, dst->src);
//     EXPECT_EQ(src.dst, dst->dst);
//     EXPECT_EQ(src.index, dst->index);
//     EXPECT_EQ(src.index_type, dst->index_type);

//     EXPECT_EQ(src.num_keys, dst->num_keys);

//     for(int i = 0; i < dst->num_keys; i++) {
//         EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
//         EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);

//         EXPECT_EQ(src.value_lens[i], dst->value_lens[i]);
//         EXPECT_EQ(memcmp(src.values[i], dst->values[i], dst->value_lens[i]), 0);
//     }
// }

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

        src.key = ::operator new(KEY_LEN);
        memcpy(src.key, KEY, KEY_LEN * sizeof(char));

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

    delete dst;
    EXPECT_EQ(Memory::MESSAGE_BUFFER::Instance().used(), 0);
}

// TEST(thallium_pack_unpack, TransportBGetMessage) {
//     TransportBGetMessage src;
//     {
//         src.mtype = TransportMessageType::BGET;
//         src.src = 1;
//         src.dst = 1;
//         src.index = 1;
//         src.index_type = PRIMARY_INDEX;
//         src.index_name = nullptr;

//         src.op = TransportGetMessageOp::GET_EQ;
//         src.num_keys = 1;

//         src.keys = new void*[1]();;
//         src.keys[0] = ::operator new(KEY_LEN);
//         memcpy(src.keys[0], KEY, KEY_LEN * sizeof(char));

//         src.key_lens = new int[1]();
//         src.key_lens[0] = KEY_LEN;

//         src.num_recs = 1;
//     }

//     void *buf = nullptr;
//     int bufsize;
//     EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

//     TransportBGetMessage *dst = nullptr;
//     EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

//     ASSERT_NE(dst, nullptr);
//     EXPECT_EQ(src.mtype, dst->mtype);
//     EXPECT_EQ(src.src, dst->src);
//     EXPECT_EQ(src.dst, dst->dst);
//     EXPECT_EQ(src.index, dst->index);
//     EXPECT_EQ(src.index_type, dst->index_type);

//     EXPECT_EQ(src.num_keys, dst->num_keys);

//     for(int i = 0; i < dst->num_keys; i++) {
//         EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
//         EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);
//     }

//     EXPECT_EQ(src.num_recs, dst->num_recs);
// }

// TEST(thallium_pack_unpack, TransportDeleteMessage) {
//     TransportDeleteMessage src;
//     {
//         src.mtype = TransportMessageType::BGET;
//         src.src = 1;
//         src.dst = 1;
//         src.index = 1;
//         src.index_type = PRIMARY_INDEX;
//         src.index_name = nullptr;

//         src.key = ::operator new(KEY_LEN);
//         memcpy(src.key, KEY, KEY_LEN * sizeof(char));

//         src.key_len = KEY_LEN;
//     }

//     void *buf = nullptr;
//     int bufsize;
//     EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

//     TransportDeleteMessage *dst = nullptr;
//     EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

// //     EXPECT_EQ(src.mtype, dst->mtype);
//     EXPECT_EQ(src.src, dst->src);
//     EXPECT_EQ(src.dst, dst->dst);
//     EXPECT_EQ(src.index, dst->index);
//     EXPECT_EQ(src.index_type, dst->index_type);

//     EXPECT_EQ(src.key_len, dst->key_len);
//     EXPECT_EQ(memcmp(src.key, dst->key, dst->key_len), 0);
// }

// TEST(thallium_pack_unpack, TransportBDeleteMessage) {
//     TransportBDeleteMessage src;
//     {
//         src.mtype = TransportMessageType::BGET;
//         src.src = 1;
//         src.dst = 1;
//         src.index = 1;
//         src.index_type = PRIMARY_INDEX;
//         src.index_name = nullptr;

//         src.num_keys = 1;

//         src.keys = new void*[1]();;
//         src.keys[0] = ::operator new(KEY_LEN);
//         memcpy(src.keys[0], KEY, KEY_LEN * sizeof(char));

//         src.key_lens = new int[1]();
//         src.key_lens[0] = KEY_LEN;
//     }

//     void *buf = nullptr;
//     int bufsize;
//     EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

//     TransportBDeleteMessage *dst = nullptr;
//     EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

//     ASSERT_NE(dst, nullptr);
//     EXPECT_EQ(src.mtype, dst->mtype);
//     EXPECT_EQ(src.src, dst->src);
//     EXPECT_EQ(src.dst, dst->dst);
//     EXPECT_EQ(src.index, dst->index);
//     EXPECT_EQ(src.index_type, dst->index_type);

//     EXPECT_EQ(src.num_keys, dst->num_keys);

//     for(int i = 0; i < dst->num_keys; i++) {
//         EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
//         EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);
//     }
// }

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
    EXPECT_EQ(Memory::MESSAGE_BUFFER::Instance().used(), 0);
}

// TEST(thallium_pack_unpack, TransportBGetRecvMessage) {
//     TransportBGetRecvMessage src;
//     {
//         src.mtype = TransportMessageType::RECV_BGET;
//         src.src = 1;
//         src.dst = 1;
//         src.index = 1;
//         src.index_type = PRIMARY_INDEX;
//         src.index_name = nullptr;

//         src.error = MDHIM_SUCCESS;

//         src.keys = new void*[1]();;
//         src.keys[0] = ::operator new(KEY_LEN);
//         memcpy(src.keys[0], KEY, KEY_LEN * sizeof(char));

//         src.key_lens = new int[1]();
//         src.key_lens[0] = KEY_LEN;

//         src.values = new void*[1]();;
//         src.values[0] = ::operator new(VALUE_LEN);
//         memcpy(src.values[0], VALUE, VALUE_LEN * sizeof(char));

//         src.value_lens = new int[1]();
//         src.value_lens[0] = VALUE_LEN;

//         src.num_keys = 1;
//         src.next = nullptr;
//     }

//     void *buf = nullptr;
//     int bufsize;
//     EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

//     TransportBGetRecvMessage *dst = nullptr;
//     EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

//     ASSERT_NE(dst, nullptr);
//     EXPECT_EQ(src.mtype, dst->mtype);
//     EXPECT_EQ(src.src, dst->src);
//     EXPECT_EQ(src.dst, dst->dst);
//     EXPECT_EQ(src.index, dst->index);
//     EXPECT_EQ(src.index_type, dst->index_type);

//     EXPECT_EQ(src.num_keys, dst->num_keys);

//     for(int i = 0; i < dst->num_keys; i++) {
//         EXPECT_EQ(src.key_lens[i], dst->key_lens[i]);
//         EXPECT_EQ(memcmp(src.keys[i], dst->keys[i], dst->key_lens[i]), 0);

//         EXPECT_EQ(src.value_lens[i], dst->value_lens[i]);
//         EXPECT_EQ(memcmp(src.values[i], dst->values[i], dst->value_lens[i]), 0);
//     }
// }

// TEST(thallium_pack_unpack, TransportBRecvMessage) {
//     TransportBRecvMessage src;
//     {
//         src.mtype = TransportMessageType::PUT;
//         src.src = 1;
//         src.dst = 1;
//         src.index = 1;
//         src.index_type = PRIMARY_INDEX;
//         src.index_name = nullptr;

//         src.error = MDHIM_SUCCESS;
//     }

//     void *buf = nullptr;
//     int bufsize;
//     EXPECT_EQ(ThalliumPacker::pack(&src, buf), MDHIM_SUCCESS);

//     TransportBRecvMessage *dst = nullptr;
//     EXPECT_EQ(ThalliumUnpacker::unpack(&dst, buf), MDHIM_SUCCESS);

//     ASSERT_NE(dst, nullptr);
//     EXPECT_EQ(src.mtype, dst->mtype);
//     EXPECT_EQ(src.src, dst->src);
//     EXPECT_EQ(src.dst, dst->dst);
//     EXPECT_EQ(src.index, dst->index);
//     EXPECT_EQ(src.index_type, dst->index_type);

//     EXPECT_EQ(src.error, dst->error);
// }
