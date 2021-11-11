#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

TEST(hxhim, OpenClose) {
    hxhim_t hx;
    ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(fill_options(&hx), true);
    ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
}
