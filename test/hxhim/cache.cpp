#include <gtest/gtest.h>

#include "hxhim/private/cache.hpp"

TEST(PutData, constructor) {
    hxhim::PutData put;

    EXPECT_EQ(put.subject, nullptr);
    EXPECT_EQ(put.predicate, nullptr);
    EXPECT_EQ(put.object_type, hxhim_object_type_t::HXHIM_OBJECT_TYPE_INVALID);
    EXPECT_EQ(put.object, nullptr);
}

TEST(PutData, moveto) {
    ReferenceBlob *subject   = construct<ReferenceBlob>((void *) (uintptr_t) rand(), rand());
    ReferenceBlob *predicate = construct<ReferenceBlob>((void *) (uintptr_t) rand(), rand());
    hxhim_object_type_t object_type = (hxhim_object_type_t) rand();
    ReferenceBlob *object    = construct<ReferenceBlob>((void *) (uintptr_t) rand(), rand());

    Transport::Request::BPut bput(1);

    for(int i = 0; i < 2; i++) {
        hxhim::PutData put;
        put.subject     = subject;
        put.predicate   = predicate;
        put.object_type = object_type;
        put.object      = object;

        if (i == 0) {
            EXPECT_EQ(put.moveto(&bput), HXHIM_SUCCESS);

            EXPECT_EQ(put.subject,     nullptr);
            EXPECT_EQ(put.predicate,   nullptr);
            EXPECT_EQ(put.object_type, object_type);
            EXPECT_EQ(put.object,      nullptr);

            EXPECT_EQ(bput.subjects[0],        subject);
            EXPECT_EQ(bput.predicates[0],      predicate);
            EXPECT_EQ(bput.object_types[0],    object_type);
            EXPECT_EQ(bput.objects[0],         object);
            EXPECT_EQ(bput.orig.subjects[0],   subject->data());
            EXPECT_EQ(bput.orig.predicates[0], predicate->data());
        }
        else {
            EXPECT_EQ(put.moveto(&bput), HXHIM_ERROR);
        }
    }
}

TEST(GetData, constructor) {
    hxhim::GetData get;

    EXPECT_EQ(get.subject, nullptr);
    EXPECT_EQ(get.predicate, nullptr);
    EXPECT_EQ(get.object_type, hxhim_object_type_t::HXHIM_OBJECT_TYPE_INVALID);
}

TEST(GetData, moveto) {
    ReferenceBlob *subject   = construct<ReferenceBlob>((void *) (uintptr_t) rand(), rand());
    ReferenceBlob *predicate = construct<ReferenceBlob>((void *) (uintptr_t) rand(), rand());
    hxhim_object_type_t object_type = (hxhim_object_type_t) rand();

    Transport::Request::BGet bget(1);

    for(int i = 0; i < 2; i++) {
        hxhim::GetData get;
        get.subject     = subject;
        get.predicate   = predicate;
        get.object_type = object_type;

        if (i == 0) {
            EXPECT_EQ(get.moveto(&bget), HXHIM_SUCCESS);

            EXPECT_EQ(get.subject,     nullptr);
            EXPECT_EQ(get.predicate,   nullptr);
            EXPECT_EQ(get.object_type, object_type);

            EXPECT_EQ(bget.subjects[0],        subject);
            EXPECT_EQ(bget.predicates[0],      predicate);
            EXPECT_EQ(bget.object_types[0],    object_type);
            EXPECT_EQ(bget.orig.subjects[0],   subject->data());
            EXPECT_EQ(bget.orig.predicates[0], predicate->data());
        }
        else {
            EXPECT_EQ(get.moveto(&bget), HXHIM_ERROR);
        }
    }
}


TEST(GetOpData, constructor) {
    hxhim::GetOpData getop;

    EXPECT_EQ(getop.subject, nullptr);
    EXPECT_EQ(getop.predicate, nullptr);
    EXPECT_EQ(getop.object_type, hxhim_object_type_t::HXHIM_OBJECT_TYPE_INVALID);
    EXPECT_EQ(getop.num_recs, 0);
    EXPECT_EQ(getop.op, hxhim_get_op_t::HXHIM_GET_INVALID);
}

TEST(GetOpData, moveto) {
    ReferenceBlob *subject   = construct<ReferenceBlob>((void *) (uintptr_t) rand(), rand());
    ReferenceBlob *predicate = construct<ReferenceBlob>((void *) (uintptr_t) rand(), rand());
    hxhim_object_type_t object_type = (hxhim_object_type_t) rand();
    std::size_t num_recs     = rand();
    hxhim_get_op_t op        = (hxhim_get_op_t) rand();

    Transport::Request::BGetOp bgetop(1);

    for(int i = 0; i < 2; i++) {
        hxhim::GetOpData getop;
        getop.subject     = subject;
        getop.predicate   = predicate;
        getop.object_type = object_type;
        getop.num_recs    = num_recs;
        getop.op          = op;

        if (i == 0) {
            EXPECT_EQ(getop.moveto(&bgetop), HXHIM_SUCCESS);

            EXPECT_EQ(getop.subject,     nullptr);
            EXPECT_EQ(getop.predicate,   nullptr);
            EXPECT_EQ(getop.object_type, object_type);
            EXPECT_EQ(getop.num_recs,    num_recs);
            EXPECT_EQ(getop.op,          op);

            EXPECT_EQ(bgetop.subjects[0],        subject);
            EXPECT_EQ(bgetop.predicates[0],      predicate);
            EXPECT_EQ(bgetop.object_types[0],    object_type);
            EXPECT_EQ(bgetop.num_recs[0],        num_recs);
            EXPECT_EQ(bgetop.ops[0],             op);
        }
        else {
            EXPECT_EQ(getop.moveto(&bgetop), HXHIM_ERROR);
        }
    }
}

TEST(DeleteData, constructor) {
    hxhim::DeleteData del;

    EXPECT_EQ(del.subject, nullptr);
    EXPECT_EQ(del.predicate, nullptr);
}

TEST(DeleteData, moveto) {
    ReferenceBlob *subject   = construct<ReferenceBlob>((void *) (uintptr_t) rand(), rand());
    ReferenceBlob *predicate = construct<ReferenceBlob>((void *) (uintptr_t) rand(), rand());

    Transport::Request::BDelete bdel(1);

    for(int i = 0; i < 2; i++) {
        hxhim::DeleteData del;
        del.subject     = subject;
        del.predicate   = predicate;

        if (i == 0) {
            EXPECT_EQ(del.moveto(&bdel), HXHIM_SUCCESS);

            EXPECT_EQ(del.subject,     nullptr);
            EXPECT_EQ(del.predicate,   nullptr);

            EXPECT_EQ(bdel.subjects[0],        subject);
            EXPECT_EQ(bdel.predicates[0],      predicate);
            EXPECT_EQ(bdel.orig.subjects[0],   subject->data());
            EXPECT_EQ(bdel.orig.predicates[0], predicate->data());
        }
        else {
            EXPECT_EQ(del.moveto(&bdel), HXHIM_ERROR);
        }
    }
}
