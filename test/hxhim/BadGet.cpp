#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

typedef uint64_t Subject_t;
typedef uint64_t Predicate_t;

TEST(hxhim, BadGet) {
    const Subject_t   SUBJECT   = (((Subject_t)   rand()) << 32) | rand();
    const Predicate_t PREDICATE = (((Predicate_t) rand()) << 32) | rand();

    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // Add nonexistant subject-predicate to get
    EXPECT_EQ(hxhim::GetDouble(&hx,
                               (void *) &SUBJECT,   sizeof(SUBJECT),   hxhim_data_t::HXHIM_DATA_BYTE,
                               (void *) &PREDICATE, sizeof(PREDICATE), hxhim_data_t::HXHIM_DATA_BYTE),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *get_results = hxhim::Flush(&hx);
    ASSERT_NE(get_results, nullptr);

    // get the results and compare them with the original data
    EXPECT_EQ(get_results->Size(), (std::size_t) 1);
    HXHIM_CXX_RESULTS_LOOP(get_results) {
        hxhim_op_t op = hxhim_op_t::HXHIM_INVALID;
        EXPECT_EQ(get_results->Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_GET);

        int status = HXHIM_ERROR;
        EXPECT_EQ(get_results->Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_ERROR);
    }

    hxhim::Results::Destroy(get_results);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
