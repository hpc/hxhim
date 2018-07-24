#include <cstdlib>
#include <ctime>
#include <type_traits>

#include <gtest/gtest.h>
#include <mpi.h>

#include "hxhim/hxhim.hpp"
#include "hxhim/private.hpp"
#include "hxhim/backend/InMemory.hpp"
#include "hxhim/config.hpp"

TEST(hxhim, Histogram) {

    hxhim_options_t opts;
    ASSERT_EQ(hxhim_options_init(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_backend(&opts, HXHIM_BACKEND_IN_MEMORY, nullptr), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_subject_type(&opts, HXHIM_SPO_INT_TYPE), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_predicate_type(&opts, HXHIM_SPO_INT_TYPE), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_object_type(&opts, HXHIM_SPO_INT_TYPE), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 10), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, TEN_BUCKETS.c_str()), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    const std::size_t subjects[]   = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    const std::size_t predicates[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    const std::size_t objects[]    = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    // Put triples
    for(std::size_t i = 0; i < 11; i++) {
        EXPECT_EQ(hxhim::Put(&hx,
                             (void *)&subjects[i], sizeof(subjects[i]),
                             (void *)&predicates[i], sizeof(predicates[i]),
                             (void *)&objects[i], sizeof(objects[i])),
                  HXHIM_SUCCESS);
    }

    // Flush all queued items
    hxhim::Results *put_results = hxhim::Flush(&hx);
    ASSERT_NE(put_results, nullptr);
    delete put_results;


    Histogram::Histogram *h = nullptr;
    EXPECT_EQ(hxhim::GetHistogram(&hx, &h), HXHIM_SUCCESS);
    ASSERT_NE(h, nullptr);
    EXPECT_EQ(h->get().size(), 10);

    // first 9 buckets each have 1 value
    for(std::size_t i = 0; i < 9; i++) {
        EXPECT_EQ(h->get().at(i), 1);
    }

    // last bucket has 2 values
    EXPECT_EQ(h->get().at(9), 2);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
