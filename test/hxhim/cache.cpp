#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/private/cache.hpp"

// subject and predicate must be valid
static const char SUBJECT[]   = "SUBJECT";
static const char PREDICATE[] = "PREDICATE";

static Blob       subject     = ReferenceBlob((void *) SUBJECT, strlen(SUBJECT));
static Blob       predicate   = ReferenceBlob((void *) PREDICATE, strlen(PREDICATE));
static const hxhim_object_type_t object_type = (hxhim_object_type_t) rand();
static Blob       object      = ReferenceBlob((void *) (uintptr_t) rand(), rand());

TEST(PutData, moveto) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    Transport::Request::BPut bput(1);

    for(int i = 0; i < 2; i++) {
        hxhim::PutData put(&hx, subject, predicate, object_type, object);

        if (i == 0) {
            EXPECT_EQ(put.moveto(&bput), HXHIM_SUCCESS);

            EXPECT_EQ(put.subject.data(),     subject.data());
            EXPECT_EQ(put.predicate.data(),   predicate.data());
            EXPECT_EQ(put.object_type,        object_type);
            EXPECT_EQ(put.object.data(),      object.data());

            EXPECT_EQ(bput.subjects[0].data(),   subject.data());
            EXPECT_EQ(bput.predicates[0].data(), predicate.data());
            EXPECT_EQ(bput.object_types[0],      object_type);
            EXPECT_EQ(bput.objects[0].data(),    object.data());
            EXPECT_EQ(bput.orig.subjects[0],     subject.data());
            EXPECT_EQ(bput.orig.predicates[0],   predicate.data());
        }
        else {
            EXPECT_EQ(put.moveto(&bput), HXHIM_ERROR);
        }
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(GetData, moveto) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    Transport::Request::BGet bget(1);

    for(int i = 0; i < 2; i++) {
        hxhim::GetData get(&hx, subject, predicate, object_type);

        if (i == 0) {
            EXPECT_EQ(get.moveto(&bget), HXHIM_SUCCESS);

            EXPECT_EQ(get.subject.data(),     subject.data());
            EXPECT_EQ(get.predicate.data(),   predicate.data());
            EXPECT_EQ(get.object_type, object_type);

            EXPECT_EQ(bget.subjects[0].data(),   subject.data());
            EXPECT_EQ(bget.predicates[0].data(), predicate.data());
            EXPECT_EQ(bget.object_types[0],      object_type);
            EXPECT_EQ(bget.orig.subjects[0],     subject.data());
            EXPECT_EQ(bget.orig.predicates[0],   predicate.data());
        }
        else {
            EXPECT_EQ(get.moveto(&bget), HXHIM_ERROR);
        }
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(GetOpData, moveto) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    std::size_t num_recs            = rand();
    hxhim_getop_t op                = (hxhim_getop_t) rand();

    Transport::Request::BGetOp bgetop(1);

    for(int i = 0; i < 2; i++) {
        hxhim::GetOpData getop(&hx, subject, predicate, object_type, num_recs, op);

        if (i == 0) {
            EXPECT_EQ(getop.moveto(&bgetop), HXHIM_SUCCESS);

            EXPECT_EQ(getop.subject.data(),   subject.data());
            EXPECT_EQ(getop.predicate.data(), predicate.data());
            EXPECT_EQ(getop.object_type,      object_type);
            EXPECT_EQ(getop.num_recs,         num_recs);
            EXPECT_EQ(getop.op,               op);

            EXPECT_EQ(bgetop.subjects[0].data(),   subject.data());
            EXPECT_EQ(bgetop.predicates[0].data(), predicate.data());
            EXPECT_EQ(bgetop.object_types[0],      object_type);
            EXPECT_EQ(bgetop.num_recs[0],          num_recs);
            EXPECT_EQ(bgetop.ops[0],               op);
        }
        else {
            EXPECT_EQ(getop.moveto(&bgetop), HXHIM_ERROR);
        }
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(DeleteData, moveto) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    Transport::Request::BDelete bdel(1);

    for(int i = 0; i < 2; i++) {
        hxhim::DeleteData del(&hx, subject, predicate);

        if (i == 0) {
            EXPECT_EQ(del.moveto(&bdel), HXHIM_SUCCESS);

            EXPECT_EQ(del.subject.data(),     subject.data());
            EXPECT_EQ(del.predicate.data(),   predicate.data());

            EXPECT_EQ(bdel.subjects[0].data(),   subject.data());
            EXPECT_EQ(bdel.predicates[0].data(), predicate.data());
            EXPECT_EQ(bdel.orig.subjects[0],     subject.data());
            EXPECT_EQ(bdel.orig.predicates[0],   predicate.data());
        }
        else {
            EXPECT_EQ(del.moveto(&bdel), HXHIM_ERROR);
        }
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
