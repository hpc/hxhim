#include <cstdio>
#include <sstream>
#include <unistd.h>

#include "gtest/gtest.h"

#include "transport/Messages/Messages.hpp"

static const char *SUBJECT = "SUBJECT";
static const std::size_t SUBJECT_LEN = strlen(SUBJECT);
static const char *PREDICATE = "PREDICATE";
static const std::size_t PREDICATE_LEN = strlen(PREDICATE);
static const hxhim_type_t OBJECT_TYPE = HXHIM_BYTE_TYPE;
static const char *OBJECT = "OBJECT";
static const std::size_t OBJECT_LEN = strlen(OBJECT);

static const std::size_t ALLOC_SIZE = 256;
static const std::size_t REGIONS = 64;

using namespace ::Transport;

TEST(Request, BPut) {
    Request::BPut src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {

        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.ds_offsets[0] = rand();

        src.subjects[0] = construct<ReferenceBlob>(&SUBJECT, SUBJECT_LEN);
        src.predicates[0] = construct<ReferenceBlob>(&PREDICATE, PREDICATE_LEN);
        src.object_types[0] = OBJECT_TYPE;
        src.objects[0] = construct<ReferenceBlob>(&OBJECT, OBJECT_LEN);
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::BPUT);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Request::BPut *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);

        EXPECT_EQ(src.subjects[i]->len, dst->subjects[i]->len);
        EXPECT_EQ(memcmp(src.subjects[i]->ptr, dst->subjects[i]->ptr, dst->subjects[i]->len), 0);

        EXPECT_EQ(src.predicates[i]->len, dst->predicates[i]->len);
        EXPECT_EQ(memcmp(src.predicates[i]->ptr, dst->predicates[i]->ptr, dst->predicates[i]->len), 0);

        EXPECT_EQ(src.object_types[i], dst->object_types[i]);
        EXPECT_EQ(src.objects[i]->len, dst->objects[i]->len);
        EXPECT_EQ(memcmp(src.objects[i]->ptr, dst->objects[i]->ptr, dst->objects[i]->len), 0);
    }

    destruct(dst);
}

TEST(Request, BGet) {
    Request::BGet src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.ds_offsets[0] = rand();

        src.subjects[0] = construct<ReferenceBlob>(&SUBJECT, SUBJECT_LEN);
        src.predicates[0] = construct<ReferenceBlob>(&PREDICATE, PREDICATE_LEN);
        src.object_types[0] = OBJECT_TYPE;
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::BGET);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Request::BGet *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);

        EXPECT_EQ(src.subjects[i]->len, dst->subjects[i]->len);
        EXPECT_EQ(memcmp(src.subjects[i]->ptr, dst->subjects[i]->ptr, dst->subjects[i]->len), 0);

        EXPECT_EQ(src.predicates[i]->len, dst->predicates[i]->len);
        EXPECT_EQ(memcmp(src.predicates[i]->ptr, dst->predicates[i]->ptr, dst->predicates[i]->len), 0);

        EXPECT_EQ(src.object_types[i], dst->object_types[i]);
    }

    destruct(dst);
}

TEST(Request, BGetOp) {
    Request::BGetOp src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.ds_offsets[0] = rand();

        src.subjects[0] = construct<ReferenceBlob>(&SUBJECT, SUBJECT_LEN);
        src.predicates[0] = construct<ReferenceBlob>(&PREDICATE, PREDICATE_LEN);
        src.object_types[0] = OBJECT_TYPE;
        src.num_recs[0] = rand();
        src.ops[0] = HXHIM_GET_EQ;
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::BGETOP);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Request::BGetOp *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);

        EXPECT_EQ(src.subjects[i]->len, dst->subjects[i]->len);
        EXPECT_EQ(memcmp(src.subjects[i]->ptr, dst->subjects[i]->ptr, dst->subjects[i]->len), 0);

        EXPECT_EQ(src.predicates[i]->len, dst->predicates[i]->len);
        EXPECT_EQ(memcmp(src.predicates[i]->ptr, dst->predicates[i]->ptr, dst->predicates[i]->len), 0);

        EXPECT_EQ(src.object_types[i], dst->object_types[i]);

        EXPECT_EQ(src.num_recs[i], dst->num_recs[i]);
        EXPECT_EQ(src.ops[i], dst->ops[i]);
    }

    destruct(dst);
}

TEST(Request, BDelete) {
    Request::BDelete src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.ds_offsets[0] = rand();

        src.subjects[0] = construct<ReferenceBlob>(&SUBJECT, SUBJECT_LEN);
        src.predicates[0] = construct<ReferenceBlob>(&PREDICATE, PREDICATE_LEN);
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::BDELETE);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Request::BDelete *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);


    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);

        EXPECT_EQ(src.subjects[i]->len, dst->subjects[i]->len);
        EXPECT_EQ(memcmp(src.subjects[i]->ptr, dst->subjects[i]->ptr, dst->subjects[i]->len), 0);

        EXPECT_EQ(src.predicates[i]->len, dst->predicates[i]->len);
        EXPECT_EQ(memcmp(src.predicates[i]->ptr, dst->predicates[i]->ptr, dst->predicates[i]->len), 0);
    }

    destruct(dst);
}

TEST(Request, BHistogram) {
    Request::BHistogram src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.ds_offsets[0] = rand();
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::BHISTOGRAM);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Request::BHistogram *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);
    }

    destruct(dst);
}

TEST(Response, BPut) {
    Response::BPut src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0] = HXHIM_SUCCESS;

        src.ds_offsets[0] = rand();

        src.orig.subjects[0]   = construct<ReferenceBlob>(&SUBJECT, SUBJECT_LEN);
        src.orig.predicates[0] = construct<ReferenceBlob>(&PREDICATE, PREDICATE_LEN);
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::BPUT);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Response::BPut *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);

        EXPECT_EQ(src.orig.subjects[i]->ptr,   dst->orig.subjects[i]->ptr);
        EXPECT_EQ(src.orig.subjects[i]->len,   dst->orig.subjects[i]->len);
        EXPECT_EQ(src.orig.predicates[i]->ptr, dst->orig.predicates[i]->ptr);
        EXPECT_EQ(src.orig.predicates[i]->len, dst->orig.predicates[i]->len);
    }

    destruct(dst);
}

TEST(Response, BGet) {
    Response::BGet src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0] = HXHIM_SUCCESS;

        src.ds_offsets[0] = rand();

        src.object_types[0] = OBJECT_TYPE;
        src.objects[0] = construct<ReferenceBlob>(&OBJECT, OBJECT_LEN);

        src.orig.subjects[0]    = construct<ReferenceBlob>(&SUBJECT, SUBJECT_LEN);
        src.orig.predicates[0]  = construct<ReferenceBlob>(&PREDICATE, PREDICATE_LEN);
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::BGET);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Response::BGet *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);

        EXPECT_EQ(src.object_types[i],         dst->object_types[i]);
        EXPECT_EQ(src.objects[i]->len,         dst->objects[i]->len);
        EXPECT_EQ(memcmp(src.objects[i]->ptr,  dst->objects[i]->ptr, dst->objects[i]->len), 0);

        EXPECT_EQ(src.orig.subjects[i]->ptr,   dst->orig.subjects[i]->ptr);
        EXPECT_EQ(src.orig.subjects[i]->len,   dst->orig.subjects[i]->len);
        EXPECT_EQ(src.orig.predicates[i]->ptr, dst->orig.predicates[i]->ptr);
        EXPECT_EQ(src.orig.predicates[i]->len, dst->orig.predicates[i]->len);
    }

    destruct(dst);
}

TEST(Response, BGetOp) {
    Response::BGetOp src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0]      = HXHIM_SUCCESS;

        src.ds_offsets[0]    = rand();

        src.object_types[0]  = OBJECT_TYPE;
        src.num_recs[0]      = 1;

        src.subjects[0]      = alloc_array<Blob *>(src.num_recs[0]);
        src.predicates[0]    = alloc_array<Blob *>(src.num_recs[0]);
        src.objects[0]       = alloc_array<Blob *>(src.num_recs[0]);

        src.subjects[0][0]   = construct<ReferenceBlob>(&SUBJECT, SUBJECT_LEN);
        src.predicates[0][0] = construct<ReferenceBlob>(&PREDICATE, PREDICATE_LEN);
        src.objects[0][0]    = construct<ReferenceBlob>(&OBJECT, OBJECT_LEN);
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::BGETOP);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Response::BGetOp *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    ASSERT_NE(dst->object_types, nullptr);
    ASSERT_NE(dst->num_recs, nullptr);
    ASSERT_NE(dst->subjects, nullptr);
    ASSERT_NE(dst->predicates, nullptr);
    ASSERT_NE(dst->objects, nullptr);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);

        EXPECT_EQ(src.object_types[i], dst->object_types[i]);
        EXPECT_EQ(src.num_recs[i], dst->num_recs[i]);

        ASSERT_NE(dst->subjects[i], nullptr);
        ASSERT_NE(dst->predicates[i], nullptr);
        ASSERT_NE(dst->objects[i], nullptr);

        for(std::size_t j = 0; j < dst->num_recs[i]; j++) {
            ASSERT_NE(dst->subjects[i][j], nullptr);
            ASSERT_NE(dst->predicates[i][j], nullptr);
            ASSERT_NE(dst->objects[i][j], nullptr);

            EXPECT_EQ(src.subjects[i][j]->len, dst->subjects[i][j]->len);
            EXPECT_EQ(memcmp(src.subjects[i][j]->ptr,
                             dst->subjects[i][j]->ptr,
                             dst->subjects[i][j]->len), 0);

            EXPECT_EQ(src.predicates[i][j]->len, dst->predicates[i][j]->len);
            EXPECT_EQ(memcmp(src.predicates[i][j]->ptr,
                             dst->predicates[i][j]->ptr,
                             dst->predicates[i][j]->len), 0);

            EXPECT_EQ(src.objects[i][j]->len, dst->objects[i][j]->len);
            EXPECT_EQ(memcmp(src.objects[i][j]->ptr,
                             dst->objects[i][j]->ptr,
                             dst->objects[i][j]->len), 0);
        }
    }

    destruct(dst);
}

TEST(Response, BDelete) {
    Response::BDelete src;
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0] = HXHIM_SUCCESS;

        src.ds_offsets[0] = rand();

        src.orig.subjects[0]   = construct<ReferenceBlob>(&SUBJECT, SUBJECT_LEN);
        src.orig.predicates[0] = construct<ReferenceBlob>(&PREDICATE, PREDICATE_LEN);
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::BDELETE);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Response::BDelete *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);

        EXPECT_EQ(src.orig.subjects[i]->ptr,   dst->orig.subjects[i]->ptr);
        EXPECT_EQ(src.orig.subjects[i]->len,   dst->orig.subjects[i]->len);
        EXPECT_EQ(src.orig.predicates[i]->ptr, dst->orig.predicates[i]->ptr);
        EXPECT_EQ(src.orig.predicates[i]->len, dst->orig.predicates[i]->len);
    }

    destruct(dst);
}

TEST(Response, BHistogram) {
    for(std::size_t count = 0; count < 10; count++) {
        Response::BHistogram src;
        ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
        {
            src.src = rand();
            src.dst = rand();

            src.count = 1;

            src.statuses[0] = HXHIM_SUCCESS;

            src.ds_offsets[0] = rand();

            src.hists[0].buckets = alloc_array<double>(count);
            src.hists[0].counts = alloc_array<std::size_t>(count);
            src.hists[0].size = count;
            for(std::size_t i = 0; i < count; i++) {
                src.hists[0].buckets[i] = i;
                src.hists[0].counts[i] = i * i;
            }
        }

        EXPECT_EQ(src.direction, Message::RESPONSE);
        EXPECT_EQ(src.type, Message::BHISTOGRAM);

        void *buf = nullptr;
        std::size_t size = 0;
        EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

        Response::BHistogram *dst = nullptr;
        EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
        dealloc(buf);

        ASSERT_TRUE(dst);
        EXPECT_EQ(src.direction, dst->direction);
        EXPECT_EQ(src.type, dst->type);
        EXPECT_EQ(src.src, dst->src);
        EXPECT_EQ(src.dst, dst->dst);

        EXPECT_EQ(src.count, dst->count);

        for(std::size_t i = 0; i < dst->count; i++) {
            EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);
            EXPECT_EQ(src.statuses[i], dst->statuses[i]);

            EXPECT_EQ(src.hists[i].size, dst->hists[i].size);
            for(std::size_t j = 0; j < dst->hists[i].size; j++) {
                EXPECT_NEAR(src.hists[i].buckets[j], dst->hists[i].buckets[j], 1e-7);
                EXPECT_EQ(src.hists[i].counts[j], dst->hists[i].counts[j]);
            }
        }

        destruct(dst);
    }
}
