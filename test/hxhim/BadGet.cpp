#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

typedef uint64_t Subject_t;
typedef uint64_t Predicate_t;

TEST(hxhim, BadGet) {
    const Subject_t SUBJECT     = (((Subject_t) rand()) << 32) | rand();
    const Predicate_t PREDICATE = (((Predicate_t) rand()) << 32) | rand();

    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // Add nonexistant subject-predicate to get
    EXPECT_EQ(hxhim::GetDouble(&hx,
                               (void *)&SUBJECT, sizeof(SUBJECT),
                               (void *)&PREDICATE, sizeof(PREDICATE),
                               nullptr),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *get_results = hxhim::Flush(&hx);
    ASSERT_NE(get_results, nullptr);

    // get the results and compare them with the original data
    EXPECT_EQ(get_results->size(), (std::size_t) 1);
    for(get_results->GoToHead(); get_results->Valid(); get_results->GoToNext()) {
        hxhim::Results::Result *res = get_results->Curr();
        ASSERT_NE(res, nullptr);

        EXPECT_NE(res->status, HXHIM_SUCCESS);
        EXPECT_EQ(res->type, HXHIM_RESULT_GET);
    }

    hxhim::Results::Destroy(get_results);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
