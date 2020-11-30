#include "gtest/gtest.h"

#include "transport/Messages/Messages.hpp"

static const char         SUBJECT[]      = "SUBJECT";
static const std::size_t  SUBJECT_LEN    = strlen(SUBJECT);
static const hxhim_data_t SUBJECT_TYPE   = (hxhim_data_t) rand();
static const char         PREDICATE[]    = "PREDICATE";
static const std::size_t  PREDICATE_LEN  = strlen(PREDICATE);
static const hxhim_data_t PREDICATE_TYPE = (hxhim_data_t) rand();
static const char         OBJECT[]       = "OBJECT";
static const std::size_t  OBJECT_LEN     = strlen(OBJECT);
static const hxhim_data_t OBJECT_TYPE    = (hxhim_data_t) rand();

using namespace ::Transport;

TEST(Request, BPut) {
    Request::BPut src;
    ASSERT_NO_THROW(src.alloc(1));
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.subjects[0]   = ReferenceBlob((void *) &SUBJECT, SUBJECT_LEN, SUBJECT_TYPE);
        src.predicates[0] = ReferenceBlob((void *) &PREDICATE, PREDICATE_LEN, PREDICATE_TYPE);
        src.objects[0]    = ReferenceBlob((void *) &OBJECT, OBJECT_LEN, OBJECT_TYPE);
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.op, hxhim_op_t::HXHIM_PUT);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Request::BPut *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.op, dst->op);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.subjects[i], dst->subjects[i]);
        EXPECT_EQ(src.predicates[i], dst->predicates[i]);
        EXPECT_EQ(src.objects[i], dst->objects[i]);
    }

    destruct(dst);
}

TEST(Request, BGet) {
    Request::BGet src;
    ASSERT_NO_THROW(src.alloc(1));
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.subjects[0]     = ReferenceBlob((void *) &SUBJECT, SUBJECT_LEN, SUBJECT_TYPE);
        src.predicates[0]   = ReferenceBlob((void *) &PREDICATE, PREDICATE_LEN, PREDICATE_TYPE);
        src.object_types[0] = OBJECT_TYPE;
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.op, hxhim_op_t::HXHIM_GET);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Request::BGet *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.op, dst->op);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.subjects[i], dst->subjects[i]);
        EXPECT_EQ(src.predicates[i], dst->predicates[i]);
        EXPECT_EQ(src.object_types[i], dst->object_types[i]);
    }

    destruct(dst);
}

TEST(Request, BGetOp) {
    Request::BGetOp src;
    ASSERT_NO_THROW(src.alloc(HXHIM_GETOP_INVALID));
    {
        src.src = rand();
        src.dst = rand();

        src.count = HXHIM_GETOP_INVALID;

        for(int i = 0; i < HXHIM_GETOP_INVALID; i++) {
            src.subjects[i] = ReferenceBlob((void *) &SUBJECT, SUBJECT_LEN, SUBJECT_TYPE);
            src.predicates[i] = ReferenceBlob((void *) &PREDICATE, PREDICATE_LEN, PREDICATE_TYPE);
            src.object_types[i] = OBJECT_TYPE;
            src.num_recs[i] = rand();
            src.ops[i] = static_cast<hxhim_getop_t>(i);
        }
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.op, hxhim_op_t::HXHIM_GETOP);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Request::BGetOp *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.op, dst->op);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        if (dst->subjects[i].data()) {
            EXPECT_EQ(src.subjects[i], dst->subjects[i]);
        }

        if (dst->predicates[i].data()) {
            EXPECT_EQ(src.predicates[i], dst->predicates[i]);
        }

        EXPECT_EQ(src.object_types[i], dst->object_types[i]);
        EXPECT_EQ(src.num_recs[i], dst->num_recs[i]);
        EXPECT_EQ(src.ops[i], dst->ops[i]);
    }

    destruct(dst);
}

TEST(Request, BDelete) {
    Request::BDelete src;
    ASSERT_NO_THROW(src.alloc(1));
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.subjects[0]   = ReferenceBlob((void *) &SUBJECT, SUBJECT_LEN, SUBJECT_TYPE);
        src.predicates[0] = ReferenceBlob((void *) &PREDICATE, PREDICATE_LEN, PREDICATE_TYPE);
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.op, hxhim_op_t::HXHIM_DELETE);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Request::BDelete *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.op, dst->op);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.subjects[i], dst->subjects[i]);
        EXPECT_EQ(src.predicates[i], dst->predicates[i]);
    }

    destruct(dst);
}

TEST(Request, BHistogram) {
    const std::string TEST_HIST_NAME = "TEST_HIST_NAME";

    Request::BHistogram src;
    ASSERT_NO_THROW(src.alloc(1));
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.names[0] = ReferenceBlob((char *) TEST_HIST_NAME.data(), TEST_HIST_NAME.size(), hxhim_data_t::HXHIM_DATA_BYTE);
    }

    EXPECT_EQ(src.direction, Message::REQUEST);
    EXPECT_EQ(src.op, hxhim_op_t::HXHIM_HISTOGRAM);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Request::BHistogram *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.op, dst->op);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);
    EXPECT_EQ(memcmp(src.names[0].data(), dst->names[0].data(), dst->names[0].size()), 0);

    destruct(dst);
}

TEST(Response, BPut) {
    Response::BPut src;
    ASSERT_NO_THROW(src.alloc(1));
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0] = DATASTORE_SUCCESS;

        src.orig.subjects[0]   = ReferenceBlob((void *) &SUBJECT, SUBJECT_LEN, SUBJECT_TYPE);
        src.orig.predicates[0] = ReferenceBlob((void *) &PREDICATE, PREDICATE_LEN, PREDICATE_TYPE);
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.op, hxhim_op_t::HXHIM_PUT);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Response::BPut *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.op, dst->op);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);

        EXPECT_EQ(src.orig.subjects[i],   dst->orig.subjects[i]);
        EXPECT_EQ(src.orig.predicates[i], dst->orig.predicates[i]);
    }

    destruct(dst);
}

TEST(Response, BGet) {
    Response::BGet src;
    ASSERT_NO_THROW(src.alloc(1));
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0] = DATASTORE_SUCCESS;

        src.objects[0] = ReferenceBlob((void *) &OBJECT, OBJECT_LEN, OBJECT_TYPE);

        src.orig.subjects[0]   = ReferenceBlob((void *) &SUBJECT, SUBJECT_LEN, SUBJECT_TYPE);
        src.orig.predicates[0] = ReferenceBlob((void *) &PREDICATE, PREDICATE_LEN, PREDICATE_TYPE);
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.op, hxhim_op_t::HXHIM_GET);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Response::BGet *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.op, dst->op);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);

        EXPECT_EQ(src.objects[i], dst->objects[i]);

        EXPECT_EQ(src.orig.subjects[i],   dst->orig.subjects[i]);
        EXPECT_EQ(src.orig.predicates[i], dst->orig.predicates[i]);
    }

    destruct(dst);
}

TEST(Response, BGetOp) {
    Response::BGetOp src;
    ASSERT_NO_THROW(src.alloc(1));
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0]      = DATASTORE_SUCCESS;

        src.num_recs[0]      = 1;

        src.subjects[0]      = alloc_array<Blob>(src.num_recs[0]);
        src.predicates[0]    = alloc_array<Blob>(src.num_recs[0]);
        src.objects[0]       = alloc_array<Blob>(src.num_recs[0]);

        src.subjects[0][0]   = ReferenceBlob((void *) &SUBJECT, SUBJECT_LEN, SUBJECT_TYPE);
        src.predicates[0][0] = ReferenceBlob((void *) &PREDICATE, PREDICATE_LEN, PREDICATE_TYPE);
        src.objects[0][0]    = ReferenceBlob((void *) &OBJECT, OBJECT_LEN, OBJECT_TYPE);
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.op, hxhim_op_t::HXHIM_GETOP);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Response::BGetOp *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.op, dst->op);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    ASSERT_NE(dst->num_recs, nullptr);
    ASSERT_NE(dst->subjects, nullptr);
    ASSERT_NE(dst->predicates, nullptr);
    ASSERT_NE(dst->objects, nullptr);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);

        EXPECT_EQ(src.num_recs[i], dst->num_recs[i]);

        ASSERT_NE(dst->subjects[i], nullptr);
        ASSERT_NE(dst->predicates[i], nullptr);
        ASSERT_NE(dst->objects[i], nullptr);

        for(std::size_t j = 0; j < dst->num_recs[i]; j++) {
            EXPECT_EQ(src.subjects[i][j],   dst->subjects[i][j]);
            EXPECT_EQ(src.predicates[i][j], dst->predicates[i][j]);
            EXPECT_EQ(src.objects[i][j],    dst->objects[i][j]);
        }
    }

    destruct(dst);
}

TEST(Response, BDelete) {
    Response::BDelete src;
    ASSERT_NO_THROW(src.alloc(1));
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0] = DATASTORE_SUCCESS;

        src.orig.subjects[0]   = ReferenceBlob((void *) &SUBJECT, SUBJECT_LEN, SUBJECT_TYPE);
        src.orig.predicates[0] = ReferenceBlob((void *) &PREDICATE, PREDICATE_LEN, PREDICATE_TYPE);
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.op, hxhim_op_t::HXHIM_DELETE);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Response::BDelete *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.op, dst->op);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);

        EXPECT_EQ(src.orig.subjects[i], dst->orig.subjects[i]);
        EXPECT_EQ(src.orig.predicates[i], dst->orig.predicates[i]);
    }

    destruct(dst);
}

TEST(Response, BHistogram) {
    Response::BHistogram src;
    ASSERT_NO_THROW(src.alloc(1));
    {
        src.src = rand();
        src.dst = rand();

        src.count = 1;

        src.statuses[0] = DATASTORE_SUCCESS;

        src.histograms[0] = std::shared_ptr<Histogram::Histogram>(construct<Histogram::Histogram>(Histogram::Config{0, histogram_rice_rule, nullptr}, ""), Histogram::deleter);
        for(std::size_t i = 0; i < 10; i++) {
            src.histograms[0]->add(rand() % 10);
        }
    }

    EXPECT_EQ(src.direction, Message::RESPONSE);
    EXPECT_EQ(src.op, hxhim_op_t::HXHIM_HISTOGRAM);

    void *buf = nullptr;
    std::size_t size = 0;
    EXPECT_EQ(Packer::pack(&src, &buf, &size), TRANSPORT_SUCCESS);

    Response::BHistogram *dst = nullptr;
    EXPECT_EQ(Unpacker::unpack(&dst, buf, size), TRANSPORT_SUCCESS);
    dealloc(buf);

    ASSERT_NE(dst, nullptr);
    EXPECT_EQ(src.direction, dst->direction);
    EXPECT_EQ(src.op, dst->op);
    EXPECT_EQ(src.src, dst->src);
    EXPECT_EQ(src.dst, dst->dst);

    EXPECT_EQ(src.count, dst->count);

    for(std::size_t i = 0; i < dst->count; i++) {
        EXPECT_EQ(src.statuses[i], dst->statuses[i]);

        const char *src_name = nullptr;
        std::size_t src_name_len = 0;
        double *src_buckets = nullptr;
        std::size_t *src_counts = nullptr;
        std::size_t src_size = 0;
        EXPECT_EQ(src.histograms[i]->get_name(&src_name, &src_name_len), HISTOGRAM_SUCCESS);
        EXPECT_EQ(src.histograms[i]->get(&src_buckets, &src_counts, &src_size), HISTOGRAM_SUCCESS);

        const char *dst_name = nullptr;
        std::size_t dst_name_len = 0;
        double *dst_buckets = nullptr;
        std::size_t *dst_counts = nullptr;
        std::size_t dst_size = 0;
        EXPECT_EQ(dst->histograms[i]->get_name(&dst_name, &dst_name_len), HISTOGRAM_SUCCESS);
        EXPECT_EQ(dst->histograms[i]->get(&dst_buckets, &dst_counts, &dst_size), HISTOGRAM_SUCCESS);

        EXPECT_EQ(std::string(src_name, src_name_len), std::string(dst_name, dst_name_len));
        EXPECT_EQ(src_size, dst_size);

        for(std::size_t i = 0; i < dst_size; i++) {
            EXPECT_EQ(src_buckets[i], dst_buckets[i]);
            EXPECT_EQ(src_counts[i],  dst_counts[i]);
        }
    }

    destruct(dst);
}
