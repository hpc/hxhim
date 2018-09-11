#include <cstdlib>
#include <ctime>
#include <type_traits>

#include <gtest/gtest.h>
#include <mpi.h>

#include "check_memory.hpp"
#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

TEST(transport, MPI) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    ASSERT_EQ(hxhim_options_set_transport_mpi(&opts, 10), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    CHECK_MEMORY(&hx);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(transport, thallium_na_sm) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, "na+sm"), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    CHECK_MEMORY(&hx);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}

TEST(transport, thallium_tcp) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);
    ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, "tcp"), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    CHECK_MEMORY(&hx);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
