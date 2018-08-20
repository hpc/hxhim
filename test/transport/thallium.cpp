#include <cstdio>
#include <sstream>
#include <unistd.h>

#include "gtest/gtest.h"

#include "transport/backend/Thallium/Thallium.hpp"
#include "utils/MemoryManager.hpp"

static const char *SUBJECT = "SUBJECT";
static const std::size_t SUBJECT_LEN = strlen(SUBJECT);
static const char *PREDICATE = "PREDICATE";
static const std::size_t PREDICATE_LEN = strlen(PREDICATE);
static const hxhim_type_t OBJECT_TYPE = HXHIM_BYTE_TYPE;
static const char *OBJECT = "OBJECT";
static const std::size_t OBJECT_LEN = strlen(OBJECT);

static const std::size_t ALLOC_SIZE = 192;
static const std::size_t REGIONS = 256;

using namespace ::Transport;
using namespace ::Transport::Thallium;

TEST(thallium_pack_unpack, RequestPut) {
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Request::Put src(fbp);
    {
        src.src = rand();
        src.dst = rand();

        src.ds_offset = rand();

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
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.ds_offset, dst->ds_offset);

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
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Request::Get src(fbp);
    {
        src.src = rand();
        src.dst = rand();

        src.ds_offset = rand();

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
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.ds_offset, dst->ds_offset);

    EXPECT_EQ(src.subject_len, dst->subject_len);
    EXPECT_EQ(memcmp(src.subject, dst->subject, dst->subject_len), 0);

    EXPECT_EQ(src.predicate_len, dst->predicate_len);
    EXPECT_EQ(memcmp(src.predicate, dst->predicate, dst->predicate_len), 0);

    EXPECT_EQ(src.object_type, dst->object_type);

    delete dst;
}

TEST(thallium_pack_unpack, RequestDelete) {
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Request::Delete src(fbp);
    {
        src.src = rand();
        src.dst = rand();

        src.ds_offset = rand();

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
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.ds_offset, dst->ds_offset);

    EXPECT_EQ(src.subject_len, dst->subject_len);
    EXPECT_EQ(memcmp(src.subject, dst->subject, dst->subject_len), 0);

    EXPECT_EQ(src.predicate_len, dst->predicate_len);
    EXPECT_EQ(memcmp(src.predicate, dst->predicate, dst->predicate_len), 0);

    delete dst;
}

TEST(thallium_pack_unpack, RequestHistogram) {
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Request::Histogram src(fbp);
    {
        src.src = rand();
        src.dst = rand();

        src.ds_offset = rand();
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::HISTOGRAM);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Request::Histogram *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.ds_offset, dst->ds_offset);

    delete dst;
}

TEST(thallium_pack_unpack, RequestBPut) {
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Request::BPut src(fbp);
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {

        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.ds_offsets[0] = rand();

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
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);

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
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Request::BGet src(fbp);
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.ds_offsets[0] = rand();

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
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);

        EXPECT_EQ(src.subject_lens[i], dst->subject_lens[i]);
        EXPECT_EQ(memcmp(src.subjects[i], dst->subjects[i], dst->subject_lens[i]), 0);

        EXPECT_EQ(src.predicate_lens[i], dst->predicate_lens[i]);
        EXPECT_EQ(memcmp(src.predicates[i], dst->predicates[i], dst->predicate_lens[i]), 0);

        EXPECT_EQ(src.object_types[i], dst->object_types[i]);
    }

    delete dst;
}

TEST(thallium_pack_unpack, RequestBGetOp) {
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Request::BGetOp src(fbp);
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.ds_offsets[0] = rand();

        src.subjects[0] = (void *) &SUBJECT;
        src.subject_lens[0] = SUBJECT_LEN;

        src.predicates[0] = (void *) &PREDICATE;
        src.predicate_lens[0] = PREDICATE_LEN;

        src.object_types[0] = OBJECT_TYPE;

        src.num_recs[0] = rand();
        src.ops[0] = HXHIM_GET_EQ;
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::BGETOP);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Request::BGetOp *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);

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
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Request::BDelete src(fbp);
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.ds_offsets[0] = rand();

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
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);

        EXPECT_EQ(src.subject_lens[i], dst->subject_lens[i]);
        EXPECT_EQ(memcmp(src.subjects[i], dst->subjects[i], dst->subject_lens[i]), 0);

        EXPECT_EQ(src.predicate_lens[i], dst->predicate_lens[i]);
        EXPECT_EQ(memcmp(src.predicates[i], dst->predicates[i], dst->predicate_lens[i]), 0);
    }

    delete dst;
}

TEST(thallium_pack_unpack, RequestBHistogram) {
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Request::BHistogram src(fbp);
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.ds_offsets[0] = rand();
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.type, Message::BHISTOGRAM);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Request::BHistogram *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);
    }

    delete dst;
}

TEST(thallium_pack_unpack, ResponsePut) {
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Response::Put src(fbp);
    {
        src.src = rand();
        src.dst = rand();

        src.ds_offset = rand();

        src.status = HXHIM_SUCCESS;
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::PUT);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::Put *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.ds_offset, dst->ds_offset);

    EXPECT_EQ(src.status, dst->status);

    delete dst;

}

TEST(thallium_pack_unpack, ResponseGet) {
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Response::Get src(fbp);
    {
        src.src = rand();
        src.dst = rand();

        src.ds_offset = rand();

        src.status = HXHIM_SUCCESS;

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
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.ds_offset, dst->ds_offset);

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
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Response::Delete src(fbp);
    {
        src.src = rand();
        src.dst = rand();

        src.ds_offset = rand();

        src.status = HXHIM_SUCCESS;
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::DELETE);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::Delete *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.ds_offset, dst->ds_offset);

    EXPECT_EQ(src.status, dst->status);

    delete dst;
}

TEST(thallium_pack_unpack, ResponseHistogram) {
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Response::Histogram src(fbp);
    {
        src.src = rand();
        src.dst = rand();

        src.ds_offset = rand();

        src.status = HXHIM_SUCCESS;

        const std::size_t count = rand() % 11;
        src.hist.buckets = new double[count]();
        src.hist.counts = new std::size_t[count]();
        src.hist.size = count;
        for(std::size_t i = 0; i < count; i++) {
            src.hist.buckets[i] = i;
            src.hist.counts[i] = i * i;
        }
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::HISTOGRAM);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::Histogram *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.ds_offset, dst->ds_offset);

    EXPECT_EQ(src.status, dst->status);

    EXPECT_EQ(src.hist.size, dst->hist.size);
    for(std::size_t i = 0; i < dst->hist.size; i++) {
        EXPECT_DOUBLE_EQ(src.hist.buckets[i], dst->hist.buckets[i]);
        EXPECT_EQ(src.hist.counts[i], dst->hist.counts[i]);
    }

    delete dst;
}

TEST(thallium_pack_unpack, ResponseBPut) {
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Response::BPut src(fbp);
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0] = HXHIM_SUCCESS;

        src.ds_offsets[0] = rand();
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::BPUT);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::BPut *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);
    }

    delete dst;
}

TEST(thallium_pack_unpack, ResponseBGet) {
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Response::BGet src(fbp);
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0] = HXHIM_SUCCESS;

        src.ds_offsets[0] = rand();

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
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);
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
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Response::BGetOp src(fbp);
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0] = HXHIM_SUCCESS;

        src.ds_offsets[0] = rand();

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
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);
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
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Response::BDelete src(fbp);
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0] = HXHIM_SUCCESS;

        src.ds_offsets[0] = rand();
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::BDELETE);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::BDelete *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);
    }

    delete dst;
}

TEST(thallium_pack_unpack, ResponseBHistogram) {
    FixedBufferPool *fbp = MemoryManager::FBP(ALLOC_SIZE, REGIONS);
    Response::BHistogram src(fbp);
    ASSERT_EQ(src.alloc(1), TRANSPORT_SUCCESS);
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0] = HXHIM_SUCCESS;

        src.ds_offsets[0] = rand();

        const std::size_t count = rand() % 11;
        src.hists[0].buckets = new double[count]();
        src.hists[0].counts = new std::size_t[count]();
        src.hists[0].size = count;
        for(std::size_t i = 0; i < count; i++) {
            src.hists[0].buckets[i] = i;
            src.hists[0].counts[i] = i * i;
        }
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.type, Message::BHISTOGRAM);
    EXPECT_EQ(src.clean, false);

    std::string buf;
    EXPECT_EQ(Packer::pack(&src, buf), TRANSPORT_SUCCESS);

    Response::BHistogram *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, fbp), TRANSPORT_SUCCESS);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.type, dst->type);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(dst->clean, true);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.ds_offsets[i], dst->ds_offsets[i]);
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);

        EXPECT_EQ(src.hists[i].size, dst->hists[i].size);
        for(std::size_t j = 0; j < dst->hists[j].size; j++) {
            EXPECT_DOUBLE_EQ(src.hists[i].buckets[j], dst->hists[i].buckets[j]);
            EXPECT_EQ(src.hists[i].counts[j], dst->hists[i].counts[j]);
        }
    }

    delete dst;
}
