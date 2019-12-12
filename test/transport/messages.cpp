#include <cstdio>
#include <sstream>
#include <unistd.h>

#include "gtest/gtest.h"

#include "transport/messages/Messages.hpp"

static const char *SUBJECT = "SUBJECT";
static const std::size_t SUBJECT_LEN = strlen(SUBJECT);
static const char *PREDICATE = "PREDICATE";
static const std::size_t PREDICATE_LEN = strlen(PREDICATE);
static const char *OBJECT = "OBJECT";
static const std::size_t OBJECT_LEN = strlen(OBJECT);

static const std::size_t ALLOC_SIZE = 256;
static const std::size_t REGIONS = 64;

TEST(SendRequestBPut, pack_unpack) {
    Transport::Request::BPut<Transport::ReferenceBlob> src(1);

    Transport::SPO <Transport::ReferenceBlob> * spo = construct <Transport::SPO <Transport::ReferenceBlob> > ();
    spo->subject = Transport::ReferenceBlob(sub, sub_len);
    spo->predicate = Transport::ReferenceBlob(pred, pred_len);
    spo->object = Transport::ReferenceBlob(obj, obj_len);
    src.add(spo);

    const std::size_t size = src.size();
    void * packed = ::operator new(size);
    memset(packed, 0, size);
    src.pack(packed, size);

    EXPECT_NO_THROW(
        {
            Transport::Request::BPut<Transport::DeepCopyBlob> b(packed, size);
        }
    );

    delete [] packed;
}

TEST(SendRequestBPut, add) {
    Transport::Request::BPut<Transport::ReferenceBlob> src(1);

    Transport::SPO <Transport::ReferenceBlob> * spo1 = construct <Transport::SPO <Transport::ReferenceBlob> > ();
    spo->subject = Transport::ReferenceBlob(sub, sub_len);
    spo->predicate = Transport::ReferenceBlob(pred, pred_len);
    spo->object = Transport::ReferenceBlob(obj, obj_len);
    EXPECT_EQ(src.add(spo1), HXHIM_SUCCESS);

    Transport::SPO <Transport::ReferenceBlob> * spo2 = construct <Transport::SPO <Transport::ReferenceBlob> > ();
    spo->subject = Transport::ReferenceBlob(sub, sub_len);
    spo->predicate = Transport::ReferenceBlob(pred, pred_len);
    spo->object = Transport::ReferenceBlob(obj, obj_len);
    EXPECT_EQ(src.add(spo2), HXHIM_ERROR);
}

TEST(SendRequestBGet, pack_unpack) {
    Transport::Request::BGet<Transport::ReferenceBlob> src(1);

    Transport::SP <Transport::ReferenceBlob> * sp = construct <Transport::SP <Transport::ReferenceBlob> > ();
    sp->subject = Transport::ReferenceBlob(sub, sub_len);
    sp->predicate = Transport::ReferenceBlob(pred, pred_len);
    src.add(sp);

    const std::size_t size = src.size();
    void * packed = ::operator new(size);
    memset(packed, 0, size);
    src.pack(packed, size);

    EXPECT_NO_THROW(
        {
            Transport::Request::BGet<Transport::DeepCopyBlob> b(packed, size);
        }
    );

    delete [] packed;
}

TEST(SendRequestBGet, add) {
    Transport::Request::BGet<Transport::ReferenceBlob> src(1);

    Transport::SP <Transport::ReferenceBlob> * sp1 = construct <Transport::SP <Transport::ReferenceBlob> > ();
    sp->subject = Transport::ReferenceBlob(sub, sub_len);
    sp->predicate = Transport::ReferenceBlob(pred, pred_len);
    EXPECT_EQ(src.add(sp1), HXHIM_SUCCESS);

    Transport::SP <Transport::ReferenceBlob> * sp2 = construct <Transport::SP <Transport::ReferenceBlob> > ();
    sp->subject = Transport::ReferenceBlob(sub, sub_len);
    sp->predicate = Transport::ReferenceBlob(pred, pred_len);
    EXPECT_EQ(src.add(sp2), HXHIM_ERROR);
}

TEST(SendRequestBDelete, pack_unpack) {
    Transport::Request::BDelete<Transport::ReferenceBlob> src(1);

    Transport::SP <Transport::ReferenceBlob> * sp = construct <Transport::SP <Transport::ReferenceBlob> > ();
    sp->subject = Transport::ReferenceBlob(sub, sub_len);
    sp->predicate = Transport::ReferenceBlob(pred, pred_len);
    src.add(sp);

    const std::size_t size = src.size();
    void * packed = ::operator new(size);
    memset(packed, 0, size);
    src.pack(packed, size);

    EXPECT_NO_THROW(
        {
            Transport::Request::BDelete<Transport::DeepCopyBlob> b(packed, size);
        }
    );

    delete [] packed;
}

TEST(SendRequestBDelete, add) {
    Transport::Request::BDelete<Transport::ReferenceBlob> src(1);

    Transport::SP <Transport::ReferenceBlob> * sp1 = construct <Transport::SP <Transport::ReferenceBlob> > ();
    sp->subject = Transport::ReferenceBlob(sub, sub_len);
    sp->predicate = Transport::ReferenceBlob(pred, pred_len);
    EXPECT_EQ(src.add(sp1), HXHIM_SUCCESS);

    Transport::SP <Transport::ReferenceBlob> * sp2 = construct <Transport::SP <Transport::ReferenceBlob> > ();
    sp->subject = Transport::ReferenceBlob(sub, sub_len);
    sp->predicate = Transport::ReferenceBlob(pred, pred_len);
    EXPECT_EQ(src.add(sp2), HXHIM_ERROR);
}
