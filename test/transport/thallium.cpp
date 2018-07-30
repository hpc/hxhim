#include <sstream>
#include <unistd.h>

#include "gtest/gtest.h"

#include "transport/backend/Thallium/Thallium.hpp"

static const char *SUBJECT = "SUBJECT";
static const std::size_t SUBJECT_LEN = strlen(SUBJECT);
static const char *PREDICATE = "PREDICATE";
static const std::size_t PREDICATE_LEN = strlen(PREDICATE);
static const hxhim_type_t OBJECT_TYPE = HXHIM_BYTE_TYPE;
static const char *OBJECT = "OBJECT";
static const std::size_t OBJECT_LEN = strlen(OBJECT);

using namespace ::Transport;
using namespace ::Transport::Thallium;

TEST(thallium_pack_unpack, RequestPut) {
    Request::Put src;
    {
        src.src = 1;
        src.dst = 1;

        src.db_offset = 1;

        src.subject = (void *) SUBJECT;
        src.subject_len = SUBJECT_LEN;

        src.predicate = (void *) PREDICATE;
        src.predicate_len = PREDICATE_LEN;

        src.object_type = OBJECT_TYPE;
        src.object = (void *) OBJECT;
        src.object_len = OBJECT_LEN;
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::PUT);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Request::Put *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.db_offset, dst->db_offset);

    EXPECT_EQ(src.subject_len, dst->subject_len);
    EXPECT_EQ(memcmp(src.subject, dst->subject, dst->subject_len), 0);

    EXPECT_EQ(src.predicate_len, dst->predicate_len);
    EXPECT_EQ(memcmp(src.predicate, dst->predicate, dst->predicate_len), 0);

    EXPECT_EQ(src.object_type, dst->object_type);
    EXPECT_EQ(src.object_len, dst->object_len);
    EXPECT_EQ(memcmp(src.object, dst->object, dst->object_len), 0);

    delete dst;
}

TEST(thallium_pack_unpack, RequestGet) {
    Request::Get src;
    {
        src.src = 1;
        src.dst = 1;

        src.db_offset = 1;

        src.subject = (void *) SUBJECT;
        src.subject_len = SUBJECT_LEN;

        src.predicate = (void *) PREDICATE;
        src.predicate_len = PREDICATE_LEN;

        src.object_type = OBJECT_TYPE;
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::GET);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Request::Get *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.db_offset, dst->db_offset);

    EXPECT_EQ(src.subject_len, dst->subject_len);
    EXPECT_EQ(memcmp(src.subject, dst->subject, dst->subject_len), 0);

    EXPECT_EQ(src.predicate_len, dst->predicate_len);
    EXPECT_EQ(memcmp(src.predicate, dst->predicate, dst->predicate_len), 0);

    EXPECT_EQ(src.object_type, dst->object_type);

    delete dst;
}

TEST(thallium_pack_unpack, RequestDelete) {
    Request::Delete src;
    {
        src.src = 1;
        src.dst = 1;

        src.db_offset = 1;

        src.subject = (void *) SUBJECT;
        src.subject_len = SUBJECT_LEN;

        src.predicate = (void *) PREDICATE;
        src.predicate_len = PREDICATE_LEN;
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::DELETE);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Request::Delete *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.db_offset, dst->db_offset);

    EXPECT_EQ(src.subject_len, dst->subject_len);
    EXPECT_EQ(memcmp(src.subject, dst->subject, dst->subject_len), 0);

    EXPECT_EQ(src.predicate_len, dst->predicate_len);
    EXPECT_EQ(memcmp(src.predicate, dst->predicate, dst->predicate_len), 0);

    delete dst;
}

TEST(thallium_pack_unpack, RequestBPut) {
    Request::BPut src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {

        src.src = 1;
        src.dst = 1;

        src.count = 1;

        src.db_offsets[0] = 1;

        src.subjects[0] = (void *) &SUBJECT;
        src.subject_lens[0] = SUBJECT_LEN;

        src.predicates[0] = (void *) &PREDICATE;
        src.predicate_lens[0] = PREDICATE_LEN;

        src.object_types[0] = OBJECT_TYPE;
        src.objects[0] = (void *) &OBJECT;
        src.object_lens[0] = OBJECT_LEN;
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::BPUT);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Request::BPut *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.db_offsets[i], dst->db_offsets[i]);

        EXPECT_EQ(src.subject_lens[i], dst->subject_lens[i]);
        EXPECT_EQ(memcmp(src.subjects[i], dst->subjects[i], dst->subject_lens[i]), 0);

        EXPECT_EQ(src.predicate_lens[i], dst->predicate_lens[i]);
        EXPECT_EQ(memcmp(src.predicates[i], dst->predicates[i], dst->predicate_lens[i]), 0);

        EXPECT_EQ(src.object_types[i], dst->object_types[i]);
        EXPECT_EQ(src.object_lens[i], dst->object_lens[i]);
        EXPECT_EQ(memcmp(src.objects[i], dst->objects[i], dst->object_lens[i]), 0);
    }

    delete dst;
}

TEST(thallium_pack_unpack, RequestBGet) {
    Request::BGet src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = 1;
        src.dst = 1;

        src.count = 1;

        src.db_offsets[0] = 1;

        src.subjects[0] = (void *) &SUBJECT;
        src.subject_lens[0] = SUBJECT_LEN;

        src.predicates[0] = (void *) &PREDICATE;
        src.predicate_lens[0] = PREDICATE_LEN;

        src.object_types[0] = OBJECT_TYPE;
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::BGET);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Request::BGet *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.db_offsets[i], dst->db_offsets[i]);

        EXPECT_EQ(src.subject_lens[i], dst->subject_lens[i]);
        EXPECT_EQ(memcmp(src.subjects[i], dst->subjects[i], dst->subject_lens[i]), 0);

        EXPECT_EQ(src.predicate_lens[i], dst->predicate_lens[i]);
        EXPECT_EQ(memcmp(src.predicates[i], dst->predicates[i], dst->predicate_lens[i]), 0);

        EXPECT_EQ(src.object_types[i], dst->object_types[i]);
    }

    delete dst;
}

TEST(thallium_pack_unpack, RequestBGetOp) {
    Request::BGetOp src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = 1;
        src.dst = 1;

        src.count = 1;

        src.db_offsets[0] = 1;

        src.subjects[0] = (void *) &SUBJECT;
        src.subject_lens[0] = SUBJECT_LEN;

        src.predicates[0] = (void *) &PREDICATE;
        src.predicate_lens[0] = PREDICATE_LEN;

        src.object_types[0] = OBJECT_TYPE;

        src.num_recs[0] = 1;
        src.ops[0] = HXHIM_GET_EQ;
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::BGETOP);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Request::BGetOp *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.db_offsets[i], dst->db_offsets[i]);

        EXPECT_EQ(src.subject_lens[i], dst->subject_lens[i]);
        EXPECT_EQ(memcmp(src.subjects[i], dst->subjects[i], dst->subject_lens[i]), 0);

        EXPECT_EQ(src.predicate_lens[i], dst->predicate_lens[i]);
        EXPECT_EQ(memcmp(src.predicates[i], dst->predicates[i], dst->predicate_lens[i]), 0);

        EXPECT_EQ(src.object_types[i], dst->object_types[i]);

        EXPECT_EQ(src.num_recs[i], dst->num_recs[i]);
        EXPECT_EQ(src.ops[i], dst->ops[i]);
    }

    delete dst;
}

TEST(thallium_pack_unpack, RequestBDelete) {
    Request::BDelete src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = 1;
        src.dst = 1;

        src.count = 1;

        src.db_offsets[0] = 1;

        src.subjects[0] = (void *) &SUBJECT;
        src.subject_lens[0] = SUBJECT_LEN;

        src.predicates[0] = (void *) &PREDICATE;
        src.predicate_lens[0] = PREDICATE_LEN;
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::BDELETE);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Request::BDelete *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.db_offsets[i], dst->db_offsets[i]);

        EXPECT_EQ(src.subject_lens[i], dst->subject_lens[i]);
        EXPECT_EQ(memcmp(src.subjects[i], dst->subjects[i], dst->subject_lens[i]), 0);

        EXPECT_EQ(src.predicate_lens[i], dst->predicate_lens[i]);
        EXPECT_EQ(memcmp(src.predicates[i], dst->predicates[i], dst->predicate_lens[i]), 0);
    }

    delete dst;
}

TEST(thallium_pack_unpack, ResponsePut) {
    Response::Put src;
    {
        src.src = 1;
        src.dst = 1;

        src.db_offset = 1;

        src.status = TRANSPORT_SUCCESS;
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::PUT);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::Put *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.db_offset, dst->db_offset);

    EXPECT_EQ(src.status, dst->status);

    delete dst;

}

TEST(thallium_pack_unpack, ResponseGet) {
    Response::Get src;
    {
        src.src = 1;
        src.dst = 1;

        src.db_offset = 1;

        src.status = TRANSPORT_SUCCESS;

        src.subject = (void *) SUBJECT;
        src.subject_len = SUBJECT_LEN;

        src.predicate = (void *) PREDICATE;
        src.predicate_len = PREDICATE_LEN;

        src.object_type = OBJECT_TYPE;

        src.object = (void *) OBJECT;
        src.object_len = OBJECT_LEN;
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::GET);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::Get *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.db_offset, dst->db_offset);

    EXPECT_EQ(src.status, dst->status);

    EXPECT_EQ(src.subject_len, dst->subject_len);
    EXPECT_EQ(memcmp(src.subject, dst->subject, dst->subject_len), 0);

    EXPECT_EQ(src.predicate_len, dst->predicate_len);
    EXPECT_EQ(memcmp(src.predicate, dst->predicate, dst->predicate_len), 0);

    EXPECT_EQ(src.object_type, dst->object_type);
    EXPECT_EQ(src.object_len, dst->object_len);
    EXPECT_EQ(memcmp(src.object, dst->object, dst->object_len), 0);

    delete dst;
}

TEST(thallium_pack_unpack, ResponseDelete) {
    Response::Delete src;
    {
        src.src = 1;
        src.dst = 1;

        src.db_offset = 1;

        src.status = TRANSPORT_SUCCESS;
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::DELETE);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::Delete *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.db_offset, dst->db_offset);

    EXPECT_EQ(src.status, dst->status);

    delete dst;
}

TEST(thallium_pack_unpack, ResponseBPut) {
    Response::BPut src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = 1;
        src.dst = 1;

        src.count = 1;

        src.statuses[0] = TRANSPORT_SUCCESS;

        src.db_offsets[0] = 1;
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::BPUT);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::BPut *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.db_offsets[i], dst->db_offsets[i]);
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);
    }

    delete dst;
}

TEST(thallium_pack_unpack, ResponseBGet) {
    Response::BGet src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = 1;
        src.dst = 1;

        src.count = 1;

        src.statuses[0] = TRANSPORT_SUCCESS;

        src.db_offsets[0] = 1;

        src.subjects[0] = (void *) &SUBJECT;
        src.subject_lens[0] = SUBJECT_LEN;

        src.predicates[0] = (void *) &PREDICATE;
        src.predicate_lens[0] = PREDICATE_LEN;

        src.object_types[0] = OBJECT_TYPE;
        src.objects[0] = (void *) &OBJECT;
        src.object_lens[0] = OBJECT_LEN;
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::BGET);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::BGet *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.db_offsets[i], dst->db_offsets[i]);
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);

        EXPECT_EQ(src.subject_lens[i], dst->subject_lens[i]);
        EXPECT_EQ(memcmp(src.subjects[i], dst->subjects[i], dst->subject_lens[i]), 0);

        EXPECT_EQ(src.predicate_lens[i], dst->predicate_lens[i]);
        EXPECT_EQ(memcmp(src.predicates[i], dst->predicates[i], dst->predicate_lens[i]), 0);

        EXPECT_EQ(src.object_types[i], dst->object_types[i]);
        EXPECT_EQ(src.object_lens[i], dst->object_lens[i]);
        EXPECT_EQ(memcmp(src.objects[i], dst->objects[i], dst->object_lens[i]), 0);
    }

    delete dst;
}

TEST(thallium_pack_unpack, ResponseBGetOp) {
    Response::BGetOp src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = 1;
        src.dst = 1;

        src.count = 1;

        src.statuses[0] = TRANSPORT_SUCCESS;

        src.db_offsets[0] = 1;

        src.subjects[0] = (void *) &SUBJECT;
        src.subject_lens[0] = SUBJECT_LEN;

        src.predicates[0] = (void *) &PREDICATE;
        src.predicate_lens[0] = PREDICATE_LEN;

        src.object_types[0] = OBJECT_TYPE;
        src.objects[0] = (void *) &OBJECT;
        src.object_lens[0] = OBJECT_LEN;
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::BGETOP);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::BGetOp *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.db_offsets[i], dst->db_offsets[i]);
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);

        EXPECT_EQ(src.subject_lens[i], dst->subject_lens[i]);
        EXPECT_EQ(memcmp(src.subjects[i], dst->subjects[i], dst->subject_lens[i]), 0);

        EXPECT_EQ(src.predicate_lens[i], dst->predicate_lens[i]);
        EXPECT_EQ(memcmp(src.predicates[i], dst->predicates[i], dst->predicate_lens[i]), 0);

        EXPECT_EQ(src.object_types[i], dst->object_types[i]);
        EXPECT_EQ(src.object_lens[i], dst->object_lens[i]);
        EXPECT_EQ(memcmp(src.objects[i], dst->objects[i], dst->object_lens[i]), 0);
    }

    delete dst;
}

TEST(thallium_pack_unpack, ResponseBDelete) {
    Response::BDelete src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = 1;
        src.dst = 1;

        src.count = 1;

        src.statuses[0] = TRANSPORT_SUCCESS;

        src.db_offsets[0] = 1;
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::BDELETE);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::BDelete *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.db_offsets[i], dst->db_offsets[i]);
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);
    }

    delete dst;
}
