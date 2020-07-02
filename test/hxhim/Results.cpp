#include <gtest/gtest.h>

#include "hxhim/Results.hpp"
#include "utils/memory.hpp"

struct TestPut : hxhim::Results::Result {
    TestPut()
        : Result(nullptr, hxhim_result_type::HXHIM_RESULT_NONE, -1, HXHIM_SUCCESS)
    {
        type = HXHIM_RESULT_PUT;
    }
};

struct TestGet : hxhim::Results::Result {
    TestGet()
        : Result(nullptr, hxhim_result_type::HXHIM_RESULT_NONE, -1, HXHIM_SUCCESS)
    {
        type = HXHIM_RESULT_GET;
    }
};

struct TestDelete : hxhim::Results::Result {
    TestDelete()
        : Result(nullptr, hxhim_result_type::HXHIM_RESULT_NONE, -1, HXHIM_SUCCESS)
    {
        type = HXHIM_RESULT_DEL;
    }
};

TEST(Results, PUT_GET_DEL) {
    hxhim::Results results(nullptr);

    // nothing works yet since there is no data
    EXPECT_EQ(results.GoToHead(), nullptr);
    EXPECT_EQ(results.GoToNext(), nullptr);
    EXPECT_EQ(results.Valid(), false);

    // add some data
    hxhim::Results::Result *put = results.Add(construct<TestPut>());
    EXPECT_NE(put, nullptr);
    hxhim::Results::Result *get = results.Add(construct<TestGet>());
    EXPECT_NE(get, nullptr);
    hxhim::Results::Result *del = results.Add(construct<TestDelete>());
    EXPECT_NE(del, nullptr);

    EXPECT_EQ(results.Valid(), false);  // still not valid because current result has not been set yet
    EXPECT_EQ(results.GoToHead(), put);
    EXPECT_EQ(results.Valid(), true);   // valid now because current result is pointing to the head of the list
    EXPECT_EQ(results.GoToNext(), get);
    EXPECT_EQ(results.GoToNext(), del);
}

TEST(Results, Loop) {
    hxhim::Results results(nullptr);

    // empty
    {
        const std::size_t size = results.Size();
        EXPECT_EQ(size, 0U);

        // check the data
        std::size_t count = 0;
        for(results.GoToHead(); results.Valid(); results.GoToNext()) {
            count++;
        }

        EXPECT_EQ(count, size);
    }

    // has stuff
    {
        // add some data
        const std::size_t puts = 10;
        for(std::size_t i = 0; i < puts; i++) {
            hxhim::Results::Result *put = results.Add(construct<TestPut>());
            EXPECT_NE(put, nullptr);
        }

        const std::size_t size = results.Size();
        EXPECT_EQ(size, 10U);

        // check the data
        std::size_t count = 0;
        for(results.GoToHead(); results.Valid(); results.GoToNext()) {
            EXPECT_EQ(results.Curr()->type, HXHIM_RESULT_PUT);
            count++;
        }

        EXPECT_EQ(count, size);
    }
}

TEST(Results, Append_Empty) {
    hxhim::Results results(nullptr);

    // add some data
    hxhim::Results::Result *put = results.Add(construct<TestPut>());
    EXPECT_NE(put, nullptr);
    hxhim::Results::Result *get = results.Add(construct<TestGet>());
    EXPECT_NE(get, nullptr);
    hxhim::Results::Result *del = results.Add(construct<TestDelete>());
    EXPECT_NE(del, nullptr);

    // append empty set of results
    hxhim::Results empty(nullptr);
    results.Append(&empty);

    EXPECT_EQ(results.GoToHead(), put);     // first result is PUT
    EXPECT_EQ(results.GoToNext(), get);     // next result is GET
    EXPECT_EQ(results.GoToNext(), del);     // next result is DEL
    EXPECT_EQ(results.GoToNext(), nullptr); // next result does not exist
    EXPECT_EQ(results.Valid(), false);
}

TEST(Results, Empty_Append) {
    hxhim::Results results(nullptr);

    // add some data to the non-empty results
    hxhim::Results::Result *put = results.Add(construct<TestPut>());
    EXPECT_NE(put, nullptr);
    hxhim::Results::Result *get = results.Add(construct<TestGet>());
    EXPECT_NE(get, nullptr);
    hxhim::Results::Result *del = results.Add(construct<TestDelete>());
    EXPECT_NE(del, nullptr);

    // empty append set of results
    hxhim::Results empty(nullptr);
    empty.Append(&results);

    EXPECT_EQ(empty.GoToHead(), put);     // first result is PUT
    EXPECT_EQ(empty.GoToNext(), get);     // next result is GET
    EXPECT_EQ(empty.GoToNext(), del);     // next result is DEL
    EXPECT_EQ(empty.GoToNext(), nullptr); // next result does not exist
    EXPECT_EQ(empty.Valid(), false);

    // appending moves the contents of the result list
    EXPECT_EQ(results.GoToHead(), nullptr);
}
