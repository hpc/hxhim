#include <gtest/gtest.h>

#include "hxhim/private/cache.hpp"

TEST(UserData, constructor) {
    hxhim::UserData ud;
    EXPECT_EQ(ud.ds_id,     -1);
    EXPECT_EQ(ud.ds_rank,   -1);
    EXPECT_EQ(ud.ds_offset, -1);
}

TEST(SubjectPredicate, constructor) {
    hxhim::SubjectPredicate sp;
    EXPECT_EQ(sp.ds_id,     -1);
    EXPECT_EQ(sp.ds_rank,   -1);
    EXPECT_EQ(sp.ds_offset, -1);

    EXPECT_EQ(sp.subject.data(),   nullptr);
    EXPECT_EQ(sp.predicate.data(), nullptr);
}

TEST(PutData, constructor) {
    hxhim::PutData put;

    EXPECT_EQ(put.subject.data(), nullptr);
    EXPECT_EQ(put.predicate.data(), nullptr);
    EXPECT_EQ(put.object_type, hxhim_object_type_t::HXHIM_OBJECT_TYPE_INVALID);
    EXPECT_EQ(put.object.data(), nullptr);
}

TEST(PutData, moveto) {
    Blob subject                    = ReferenceBlob((void *) (uintptr_t) rand(), rand());
    Blob predicate                  = ReferenceBlob((void *) (uintptr_t) rand(), rand());
    hxhim_object_type_t object_type = (hxhim_object_type_t) rand();
    Blob object                     = ReferenceBlob((void *) (uintptr_t) rand(), rand());

    Transport::Request::BPut bput(1);

    for(int i = 0; i < 2; i++) {
        hxhim::PutData put;
        put.subject     = subject;
        put.predicate   = predicate;
        put.object_type = object_type;
        put.object      = object;

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
}

TEST(GetData, constructor) {
    hxhim::GetData get;

    EXPECT_EQ(get.subject.data(), nullptr);
    EXPECT_EQ(get.predicate.data(), nullptr);
    EXPECT_EQ(get.object_type, hxhim_object_type_t::HXHIM_OBJECT_TYPE_INVALID);
}

TEST(GetData, moveto) {
    Blob subject   = ReferenceBlob((void *) (uintptr_t) rand(), rand());
    Blob predicate = ReferenceBlob((void *) (uintptr_t) rand(), rand());
    hxhim_object_type_t object_type = (hxhim_object_type_t) rand();

    Transport::Request::BGet bget(1);

    for(int i = 0; i < 2; i++) {
        hxhim::GetData get;
        get.subject     = subject;
        get.predicate   = predicate;
        get.object_type = object_type;

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
}


TEST(GetOpData, constructor) {
    hxhim::GetOpData getop;

    EXPECT_EQ(getop.subject.data(), nullptr);
    EXPECT_EQ(getop.predicate.data(), nullptr);
    EXPECT_EQ(getop.object_type, hxhim_object_type_t::HXHIM_OBJECT_TYPE_INVALID);
    EXPECT_EQ(getop.num_recs, 0);
    EXPECT_EQ(getop.op, hxhim_get_op_t::HXHIM_GET_INVALID);
}

TEST(GetOpData, moveto) {
    Blob subject                    = ReferenceBlob((void *) (uintptr_t) rand(), rand());
    Blob predicate                  = ReferenceBlob((void *) (uintptr_t) rand(), rand());
    hxhim_object_type_t object_type = (hxhim_object_type_t) rand();
    std::size_t num_recs            = rand();
    hxhim_get_op_t op               = (hxhim_get_op_t) rand();

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
}

TEST(DeleteData, constructor) {
    hxhim::DeleteData del;

    EXPECT_EQ(del.subject.data(), nullptr);
    EXPECT_EQ(del.predicate.data(), nullptr);
}

TEST(DeleteData, moveto) {
    Blob subject   = ReferenceBlob((void *) (uintptr_t) rand(), rand());
    Blob predicate = ReferenceBlob((void *) (uintptr_t) rand(), rand());

    Transport::Request::BDelete bdel(1);

    for(int i = 0; i < 2; i++) {
        hxhim::DeleteData del;
        del.subject     = subject;
        del.predicate   = predicate;

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
}
