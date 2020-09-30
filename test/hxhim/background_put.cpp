#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"

typedef uint64_t Subject_t;
typedef uint64_t Predicate_t;
typedef double   Object_t;

TEST(hxhim, background_put) {
    const Subject_t   SUBJECT   = (((Subject_t)   rand()) << 32) | rand();
    const Predicate_t PREDICATE = (((Predicate_t) rand()) << 32) | rand();
    const Object_t    OBJECT    = (((Object_t) SUBJECT) * ((Object_t) SUBJECT)) / (((Object_t) PREDICATE) * ((Object_t) PREDICATE));

    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    // automatically flush after 1 item has been PUT
    ASSERT_EQ(hxhim_options_set_start_async_put_at(&opts, 1), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // PUT triple
    EXPECT_EQ(hxhim::Put(&hx,
                         (void *) &SUBJECT, sizeof(SUBJECT),
                         (void *) &PREDICATE, sizeof(PREDICATE),
                         hxhim_object_type_t::HXHIM_OBJECT_TYPE_DOUBLE,
                         (void *) &OBJECT, sizeof(OBJECT)),
              HXHIM_SUCCESS);

    // wait for the background thread to signal it finished
    // this should never block
    {
        std::mutex mutex;
        std::unique_lock<std::mutex> lock(mutex);
        hx.p->queues.puts.done_processing.wait(lock, [&hx](){ return hx.p->async_put.results; } );
    }

    // background PUT results should have 1 item in it
    hxhim::Results *background_put_results = hx.p->async_put.results;
    EXPECT_EQ(background_put_results->Size(), 1);

    // Make sure result is correct
    HXHIM_CXX_RESULTS_LOOP(background_put_results) {
        hxhim_op_t op;
        EXPECT_EQ(background_put_results->Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_PUT);

        int status = HXHIM_ERROR;
        EXPECT_EQ(background_put_results->Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);
    }

    hxhim::Results *put_results = hxhim::FlushPuts(&hx);
    hxhim::Results::Destroy(put_results);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
