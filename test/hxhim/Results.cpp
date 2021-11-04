#include <gtest/gtest.h>

#include "datastore/constants.hpp"
#include "hxhim/Results.hpp"
#include "hxhim/private/Results.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"

struct TestResults : public hxhim::Results {
    decltype(it)::value_type Curr() const {
        if (it == results.end()) {
            return nullptr;
        }
        return *it;
    }
};

TEST(Results, PUT_GET_DEL) {
    TestResults results;

    // nothing works yet since there is no data
    results.GoToHead();
    EXPECT_EQ(results.ValidIterator(), false);
    results.GoToNext();
    EXPECT_EQ(results.ValidIterator(), false);

    // add some data
    hxhim::Result::Result *put   = construct<hxhim::Result::Put>   (nullptr, -1, DATASTORE_SUCCESS);
    hxhim::Result::Result *get   = construct<hxhim::Result::Get>   (nullptr, -1, DATASTORE_SUCCESS);
    hxhim::Result::Result *getop = construct<hxhim::Result::GetOp> (nullptr, -1, DATASTORE_SUCCESS);
    hxhim::Result::Result *del   = construct<hxhim::Result::Delete>(nullptr, -1, DATASTORE_SUCCESS);

    results.Add(put);
    results.Add(get);
    results.Add(getop);
    results.Add(del);

    results.GoToHead();
    EXPECT_EQ(results.ValidIterator(), true);

    EXPECT_EQ(results.Curr(), put);
    results.GoToNext();

    EXPECT_EQ(results.Curr(), get);
    results.GoToNext();

    EXPECT_EQ(results.Curr(), getop);
    results.GoToNext();

    EXPECT_EQ(results.Curr(), del);
    results.GoToNext();

    EXPECT_EQ(results.Curr(), nullptr);
    EXPECT_EQ(results.ValidIterator(), false);
}

TEST(Results, Loop) {
    hxhim::Results results;

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
            results.Add(construct<hxhim::Result::Put>(nullptr, -1, DATASTORE_SUCCESS));
        }

        const std::size_t size = results.Size();
        EXPECT_EQ(size, puts);

        // check the data
        std::size_t count = 0;
        HXHIM_CXX_RESULTS_LOOP(&results) {
            hxhim_op_t op = hxhim_op_t::HXHIM_INVALID;
            EXPECT_EQ(results.Op(&op), HXHIM_SUCCESS);
            EXPECT_EQ(op, hxhim_op_t::HXHIM_PUT);
            count++;
        }

        EXPECT_EQ(count, puts);
    }
}

TEST(Results, Append_Empty) {
    TestResults results;

    // add some data
    hxhim::Result::Result *put   = construct<hxhim::Result::Put>   (nullptr, -1, DATASTORE_SUCCESS);
    hxhim::Result::Result *get   = construct<hxhim::Result::Get>   (nullptr, -1, DATASTORE_SUCCESS);
    hxhim::Result::Result *getop = construct<hxhim::Result::GetOp> (nullptr, -1, DATASTORE_SUCCESS);
    hxhim::Result::Result *del   = construct<hxhim::Result::Delete>(nullptr, -1, DATASTORE_SUCCESS);

    results.Add(put);
    results.Add(get);
    results.Add(getop);
    results.Add(del);

    // append empty set of results
    TestResults empty;
    results.Append(&empty);

    results.GoToHead();
    EXPECT_EQ(results.ValidIterator(), true);

    EXPECT_EQ(results.Curr(), put);
    results.GoToNext();

    EXPECT_EQ(results.Curr(), get);
    results.GoToNext();

    EXPECT_EQ(results.Curr(), getop);
    results.GoToNext();

    EXPECT_EQ(results.Curr(), del);
    results.GoToNext();

    EXPECT_EQ(results.Curr(), nullptr);
    EXPECT_EQ(results.ValidIterator(), false);
}

TEST(Results, Empty_Append) {
    TestResults results;

    // add some data
    hxhim::Result::Result *put   = construct<hxhim::Result::Put>   (nullptr, -1, DATASTORE_SUCCESS);
    hxhim::Result::Result *get   = construct<hxhim::Result::Get>   (nullptr, -1, DATASTORE_SUCCESS);
    hxhim::Result::Result *getop = construct<hxhim::Result::GetOp> (nullptr, -1, DATASTORE_SUCCESS);
    hxhim::Result::Result *del   = construct<hxhim::Result::Delete>(nullptr, -1, DATASTORE_SUCCESS);

    results.Add(put);
    results.Add(get);
    results.Add(getop);
    results.Add(del);

    // append empty set of results
    TestResults empty;
    empty.Append(&results);

    empty.GoToHead();
    EXPECT_EQ(empty.ValidIterator(), true);

    EXPECT_EQ(empty.Curr(), put);
    empty.GoToNext();

    EXPECT_EQ(empty.Curr(), get);
    empty.GoToNext();

    EXPECT_EQ(empty.Curr(), getop);
    empty.GoToNext();

    EXPECT_EQ(empty.Curr(), del);
    empty.GoToNext();

    EXPECT_EQ(empty.Curr(), nullptr);
    EXPECT_EQ(empty.ValidIterator(), false);
}

TEST(Results, Accessors) {
    TestResults results;

    // add some data to the non-empty results
    hxhim::Result::Put *put = construct<hxhim::Result::Put>(nullptr, -1, DATASTORE_SUCCESS);
    put->subject     = RealBlob(alloc(1), 1, hxhim_data_t::HXHIM_DATA_POINTER);
    put->predicate   = RealBlob(alloc(1), 1, hxhim_data_t::HXHIM_DATA_POINTER);
    results.Add(put);

    hxhim::Result::Get *get = construct<hxhim::Result::Get>(nullptr, -1, DATASTORE_SUCCESS);
    get->subject     = RealBlob(alloc(1), 1, hxhim_data_t::HXHIM_DATA_POINTER);
    get->predicate   = RealBlob(alloc(1), 1, hxhim_data_t::HXHIM_DATA_POINTER);
    get->object      = RealBlob(alloc(1), 1, hxhim_data_t::HXHIM_DATA_BYTE);
    results.Add(get);

    hxhim::Result::GetOp *getop = construct<hxhim::Result::GetOp>(nullptr, -1, DATASTORE_SUCCESS);
    getop->subject   = RealBlob(alloc(1), 1, hxhim_data_t::HXHIM_DATA_POINTER);
    getop->predicate = RealBlob(alloc(1), 1, hxhim_data_t::HXHIM_DATA_POINTER);
    getop->object    = RealBlob(alloc(1), 1, hxhim_data_t::HXHIM_DATA_POINTER);
    results.Add(getop);

    hxhim::Result::Delete *del = construct<hxhim::Result::Delete>(nullptr, -1, DATASTORE_SUCCESS);
    del->subject     = RealBlob(alloc(1), 1, hxhim_data_t::HXHIM_DATA_POINTER);
    del->predicate   = RealBlob(alloc(1), 1, hxhim_data_t::HXHIM_DATA_POINTER);
    results.Add(del);

    // all Results will attempt to get these variables
    hxhim_op_t op;
    int status;
    void *subject = nullptr;
    std::size_t subject_len = 0;
    hxhim_data_t subject_type;
    void *predicate = nullptr;
    std::size_t predicate_len = 0;
    hxhim_data_t predicate_type;
    void *object = nullptr;
    std::size_t object_len = 0;
    hxhim_data_t object_type;

    // PUT
    results.GoToHead();
    EXPECT_EQ(results.ValidIterator(), true);
    EXPECT_EQ(results.Curr(), put);
    {
        EXPECT_EQ(results.Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_PUT);

        EXPECT_EQ(results.Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);

        EXPECT_EQ(results.Subject(&subject, &subject_len, &subject_type), HXHIM_SUCCESS);
        EXPECT_EQ(put->subject.data(), subject);

        EXPECT_EQ(results.Predicate(&predicate, &predicate_len, &predicate_type), HXHIM_SUCCESS);
        EXPECT_EQ(put->predicate.data(), predicate);

        EXPECT_EQ(results.Object(&object, &object_len, &object_type), HXHIM_ERROR);
    }

    // GET
    results.GoToNext();
    EXPECT_EQ(results.ValidIterator(), true);
    EXPECT_EQ(results.Curr(), get);
    {
        EXPECT_EQ(results.Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_GET);

        EXPECT_EQ(results.Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);

        EXPECT_EQ(results.Subject(&subject, &subject_len, &subject_type), HXHIM_SUCCESS);
        EXPECT_EQ(get->subject.data(), subject);

        EXPECT_EQ(results.Predicate(&predicate, &predicate_len, &predicate_type), HXHIM_SUCCESS);
        EXPECT_EQ(get->predicate.data(), predicate);

        EXPECT_EQ(results.Object(&object, &object_len, &object_type), HXHIM_SUCCESS);
        EXPECT_EQ(get->object.data(), object);
    }

    // GETOP
    results.GoToNext();
    EXPECT_EQ(results.ValidIterator(), true);
    EXPECT_EQ(results.Curr(), getop);
    {
        EXPECT_EQ(results.Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_GETOP);

        EXPECT_EQ(results.Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);

        EXPECT_EQ(results.Subject(&subject, &subject_len, &subject_type), HXHIM_SUCCESS);
        EXPECT_EQ(getop->subject.data(), subject);

        EXPECT_EQ(results.Predicate(&predicate, &predicate_len, &predicate_type), HXHIM_SUCCESS);
        EXPECT_EQ(getop->predicate.data(), predicate);

        EXPECT_EQ(results.Object(&object, &object_len, &object_type), HXHIM_SUCCESS);
        EXPECT_EQ(getop->object.data(), object);
    }

    // DEL
    results.GoToNext();
    EXPECT_EQ(results.ValidIterator(), true);
    EXPECT_EQ(results.Curr(), del);
    {
        EXPECT_EQ(results.Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_DELETE);

        EXPECT_EQ(results.Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);

        EXPECT_EQ(results.Subject(&subject, &subject_len, &subject_type), HXHIM_SUCCESS);
        EXPECT_EQ(del->subject.data(), subject);

        EXPECT_EQ(results.Predicate(&predicate, &predicate_len, &predicate_type), HXHIM_SUCCESS);
        EXPECT_EQ(del->predicate.data(), predicate);

        EXPECT_EQ(results.Object(&object, &object_len, &object_type), HXHIM_ERROR);
    }

    results.GoToNext();
    EXPECT_EQ(results.ValidIterator(), false);
    EXPECT_EQ(results.Curr(), nullptr);
    {
        EXPECT_EQ(results.Op(&op), HXHIM_ERROR);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_DELETE);
        EXPECT_EQ(results.Status(&status), HXHIM_ERROR);
        EXPECT_EQ(results.Subject(&subject, &subject_len, &subject_type), HXHIM_ERROR);
        EXPECT_EQ(results.Predicate(&predicate, &predicate_len, &predicate_type), HXHIM_ERROR);
        EXPECT_EQ(results.Object(&object, &object_len, &object_type), HXHIM_ERROR);
    }
}
