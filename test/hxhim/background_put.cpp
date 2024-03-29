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

    hxhim_t hx;
    ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(fill_options(&hx), true);

    EXPECT_EQ(hx.p->async_puts.enabled, 0);
    EXPECT_EQ(hx.p->async_puts.max_queued, 0);

    // automatically flush after 1 item has been PUT
    ASSERT_EQ(hxhim_set_start_async_puts_at(&hx, 1), HXHIM_SUCCESS);

    EXPECT_EQ(hx.p->async_puts.enabled, 1);
    EXPECT_EQ(hx.p->async_puts.max_queued, 1);

    ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);

    // PUT triple
    EXPECT_EQ(hxhim::Put(&hx,
                         (void *) &SUBJECT,   sizeof(SUBJECT),   hxhim_data_t::HXHIM_DATA_UINT64,
                         (void *) &PREDICATE, sizeof(PREDICATE), hxhim_data_t::HXHIM_DATA_UINT64,
                         (void *) &OBJECT,    sizeof(OBJECT),    hxhim_data_t::HXHIM_DATA_DOUBLE,
                         HXHIM_PUT_SPO),
              HXHIM_SUCCESS);

    // force the background thread to flush
    hxhim::wait_for_background_puts(&hx, false);

    // background PUT results should have 1 item in it
    hxhim::Results *background_put_results = hx.p->async_puts.results;
    ASSERT_NE(background_put_results, nullptr);
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

    // check the results after flushing
    hxhim::Results *put_results = hxhim::FlushPuts(&hx);
    HXHIM_CXX_RESULTS_LOOP(put_results) {
        hxhim_op_t op;
        EXPECT_EQ(put_results->Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_PUT);

        int status = HXHIM_ERROR;
        EXPECT_EQ(put_results->Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);
    }
    hxhim::Results::Destroy(put_results);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
}
