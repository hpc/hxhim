#include <gtest/gtest.h>

#include "hxhim/Results.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"

TEST(Results, PUT_GET_DEL) {
    hxhim::Results results(nullptr);

    // nothing works yet since there is no data
    EXPECT_EQ(results.GoToHead(), nullptr);
    EXPECT_EQ(results.GoToNext(), nullptr);
    EXPECT_EQ(results.ValidIterator(), false);

    // add some data
    hxhim::Results::Result *put = results.Add(construct<hxhim::Results::Put>(nullptr, -1, HXHIM_SUCCESS));
    EXPECT_NE(put, nullptr);
    hxhim::Results::Result *get = results.Add(construct<hxhim::Results::Get>(nullptr, -1, HXHIM_SUCCESS));
    EXPECT_NE(get, nullptr);
    hxhim::Results::Result *getop = results.Add(construct<hxhim::Results::GetOp>(nullptr, -1, HXHIM_SUCCESS));
    EXPECT_NE(getop, nullptr);
    hxhim::Results::Result *del = results.Add(construct<hxhim::Results::Delete>(nullptr, -1, HXHIM_SUCCESS));
    EXPECT_NE(del, nullptr);

    EXPECT_EQ(results.ValidIterator(), false);  // still not valid because current result has not been set yet
    EXPECT_EQ(results.GoToHead(), put);
    EXPECT_EQ(results.ValidIterator(), true);   // valid now because current result is pointing to the head of the list
    EXPECT_EQ(results.GoToNext(), get);
    EXPECT_EQ(results.GoToNext(), getop);
    EXPECT_EQ(results.GoToNext(), del);
    EXPECT_EQ(results.GoToNext(), nullptr);
    EXPECT_EQ(results.ValidIterator(), false);
}

TEST(Results, Loop) {
    hxhim::Results results(nullptr);

    // empty
    {
        const std::size_t size = results.Size();
        EXPECT_EQ(size, 0U);

        // check the data
        std::size_t count = 0;
        HXHIM_CXX_RESULTS_LOOP(&results) {
            count++;
        }

        EXPECT_EQ(count, size);
    }

    // has stuff
    {
        // add some data
        const std::size_t puts = 10;
        for(std::size_t i = 0; i < puts; i++) {
            hxhim::Results::Result *put = results.Add(construct<hxhim::Results::Put>(nullptr, -1, HXHIM_SUCCESS));
            EXPECT_NE(put, nullptr);
        }

        const std::size_t size = results.Size();
        EXPECT_EQ(size, 10U);

        // check the data
        std::size_t count = 0;
        HXHIM_CXX_RESULTS_LOOP(&results) {
            hxhim_op_t op = hxhim_op_t::HXHIM_INVALID;
            EXPECT_EQ(results.Op(&op), HXHIM_SUCCESS);
            EXPECT_EQ(op, hxhim_op_t::HXHIM_PUT);
            count++;
        }

        EXPECT_EQ(count, size);
    }
}

TEST(Results, Append_Empty) {
    hxhim::Results results(nullptr);

    // add some data
    hxhim::Results::Result *put = results.Add(construct<hxhim::Results::Put>(nullptr, -1, HXHIM_SUCCESS));
    EXPECT_NE(put, nullptr);
    hxhim::Results::Result *get = results.Add(construct<hxhim::Results::Get>(nullptr, -1, HXHIM_SUCCESS));
    EXPECT_NE(get, nullptr);
    hxhim::Results::Result *getop = results.Add(construct<hxhim::Results::GetOp>(nullptr, -1, HXHIM_SUCCESS));
    EXPECT_NE(getop, nullptr);
    hxhim::Results::Result *del = results.Add(construct<hxhim::Results::Delete>(nullptr, -1, HXHIM_SUCCESS));
    EXPECT_NE(del, nullptr);

    // append empty set of results
    hxhim::Results empty(nullptr);
    results.Append(&empty);

    EXPECT_EQ(results.GoToHead(), put);     // first result is PUT
    EXPECT_EQ(results.GoToNext(), get);     // next result is GET
    EXPECT_EQ(results.GoToNext(), getop);   // next result is GETOP
    EXPECT_EQ(results.GoToNext(), del);     // next result is DEL
    EXPECT_EQ(results.GoToNext(), nullptr); // next result does not exist
    EXPECT_EQ(results.ValidIterator(), false);
}

TEST(Results, Empty_Append) {
    hxhim::Results results(nullptr);

    // add some data to the non-empty results
    hxhim::Results::Result *put = results.Add(construct<hxhim::Results::Put>(nullptr, -1, HXHIM_SUCCESS));
    EXPECT_NE(put, nullptr);
    hxhim::Results::Result *get = results.Add(construct<hxhim::Results::Get>(nullptr, -1, HXHIM_SUCCESS));
    EXPECT_NE(get, nullptr);
    hxhim::Results::Result *getop = results.Add(construct<hxhim::Results::GetOp>(nullptr, -1, HXHIM_SUCCESS));
    EXPECT_NE(getop, nullptr);
    hxhim::Results::Result *del = results.Add(construct<hxhim::Results::Delete>(nullptr, -1, HXHIM_SUCCESS));
    EXPECT_NE(del, nullptr);

    // empty append set of results
    hxhim::Results empty(nullptr);
    empty.Append(&results);

    EXPECT_EQ(empty.GoToHead(), put);     // first result is PUT
    EXPECT_EQ(empty.GoToNext(), get);     // next result is GET
    EXPECT_EQ(empty.GoToNext(), getop);   // next result is GETOP
    EXPECT_EQ(empty.GoToNext(), del);     // next result is DEL
    EXPECT_EQ(empty.GoToNext(), nullptr); // next result does not exist
    EXPECT_EQ(empty.ValidIterator(), false);

    // appending moves the contents of the result list
    EXPECT_EQ(results.GoToHead(), nullptr);
}

TEST(Results, Accessors) {
    hxhim::Results results(nullptr);

    // add some data to the non-empty results
    hxhim::Results::Result *rput = results.Add(construct<hxhim::Results::Put>(nullptr, -1, HXHIM_SUCCESS));
    hxhim::Results::Put *put = static_cast<hxhim::Results::Put *>(rput);
    EXPECT_NE(put, nullptr);
    put->subject = RealBlob(alloc(1), 1);
    put->predicate = RealBlob(alloc(1), 1);

    hxhim::Results::Result *rget = results.Add(construct<hxhim::Results::Get>(nullptr, -1, HXHIM_SUCCESS));
    hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(rget);
    EXPECT_NE(get, nullptr);
    get->subject = RealBlob(alloc(1), 1);
    get->predicate = RealBlob(alloc(1), 1);
    get->object_type = hxhim_object_type_t::HXHIM_OBJECT_TYPE_BYTE;
    get->object = RealBlob(alloc(1), 1);

    hxhim::Results::Result *rgetop = results.Add(construct<hxhim::Results::GetOp>(nullptr, -1, HXHIM_SUCCESS));
    hxhim::Results::GetOp *getop = static_cast<hxhim::Results::GetOp *>(rgetop);
    EXPECT_NE(getop, nullptr);
    getop->subject = RealBlob(alloc(1), 1);
    getop->predicate = RealBlob(alloc(1), 1);
    get->object_type = hxhim_object_type_t::HXHIM_OBJECT_TYPE_BYTE;
    getop->object = RealBlob(alloc(1), 1);

    hxhim::Results::Result *rdel = results.Add(construct<hxhim::Results::Delete>(nullptr, -1, HXHIM_SUCCESS));
    hxhim::Results::Delete *del = static_cast<hxhim::Results::Delete *>(rdel);
    EXPECT_NE(del, nullptr);
    del->subject = RealBlob(alloc(1), 1);
    del->predicate = RealBlob(alloc(1), 1);

    // all Results will attempt to get these variables
    hxhim_op_t op;
    int status;
    void *subject = nullptr;
    std::size_t subject_len = 0;
    void *predicate = nullptr;
    std::size_t predicate_len = 0;
    hxhim_object_type_t object_type = HXHIM_OBJECT_TYPE_INVALID;
    void *object = nullptr;
    std::size_t object_len = 0;

    EXPECT_EQ(results.GoToHead(), put);
    EXPECT_EQ(results.ValidIterator(), true);
    {
        EXPECT_EQ(results.Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_PUT);

        EXPECT_EQ(results.Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);

        EXPECT_EQ(results.Subject(&subject, &subject_len), HXHIM_SUCCESS);
        EXPECT_EQ(put->subject.data(), subject);

        EXPECT_EQ(results.Predicate(&predicate, &predicate_len), HXHIM_SUCCESS);
        EXPECT_EQ(put->predicate.data(), predicate);

        EXPECT_EQ(results.ObjectType(&object_type), HXHIM_ERROR);
        EXPECT_EQ(results.Object(&object, &object_len), HXHIM_ERROR);
    }

    EXPECT_EQ(results.GoToNext(), get);
    EXPECT_EQ(results.ValidIterator(), true);
    {
        EXPECT_EQ(results.Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_GET);

        EXPECT_EQ(results.Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);

        EXPECT_EQ(results.Subject(&subject, &subject_len), HXHIM_SUCCESS);
        EXPECT_EQ(get->subject.data(), subject);

        EXPECT_EQ(results.Predicate(&predicate, &predicate_len), HXHIM_SUCCESS);
        EXPECT_EQ(get->predicate.data(), predicate);

        EXPECT_EQ(results.ObjectType(&object_type), HXHIM_SUCCESS);
        EXPECT_EQ(get->object_type, object_type);

        EXPECT_EQ(results.Object(&object, &object_len), HXHIM_SUCCESS);
        EXPECT_EQ(get->object.data(), object);
    }

    EXPECT_EQ(results.GoToNext(), getop);
    EXPECT_EQ(results.ValidIterator(), true);
    {
        EXPECT_EQ(results.Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_GETOP);

        EXPECT_EQ(results.Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);

        EXPECT_EQ(results.Subject(&subject, &subject_len), HXHIM_SUCCESS);
        EXPECT_EQ(getop->subject.data(), subject);

        EXPECT_EQ(results.Predicate(&predicate, &predicate_len), HXHIM_SUCCESS);
        EXPECT_EQ(getop->predicate.data(), predicate);

        EXPECT_EQ(results.ObjectType(&object_type), HXHIM_SUCCESS);
        EXPECT_EQ(getop->object_type, object_type);

        EXPECT_EQ(results.Object(&object, &object_len), HXHIM_SUCCESS);
        EXPECT_EQ(getop->object.data(), object);
    }

    EXPECT_EQ(results.GoToNext(), del);
    EXPECT_EQ(results.ValidIterator(), true);
    {
        EXPECT_EQ(results.Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_DELETE);

        EXPECT_EQ(results.Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);

        EXPECT_EQ(results.Subject(&subject, &subject_len), HXHIM_SUCCESS);
        EXPECT_EQ(del->subject.data(), subject);

        EXPECT_EQ(results.Predicate(&predicate, &predicate_len), HXHIM_SUCCESS);
        EXPECT_EQ(del->predicate.data(), predicate);

        EXPECT_EQ(results.ObjectType(&object_type), HXHIM_ERROR);
        EXPECT_EQ(results.Object(&object, &object_len), HXHIM_ERROR);
    }

    EXPECT_EQ(results.GoToNext(), nullptr);
    EXPECT_EQ(results.ValidIterator(), false);
    {
        EXPECT_EQ(results.Op(&op), HXHIM_ERROR);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_DELETE);
        EXPECT_EQ(results.Status(&status), HXHIM_ERROR);
        EXPECT_EQ(results.Subject(&subject, &subject_len), HXHIM_ERROR);
        EXPECT_EQ(results.Predicate(&predicate, &predicate_len), HXHIM_ERROR);
        EXPECT_EQ(results.ObjectType(&object_type), HXHIM_ERROR);
        EXPECT_EQ(results.Object(&object, &object_len), HXHIM_ERROR);
    }
}
