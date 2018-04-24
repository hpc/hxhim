//
// Created by bws on 11/7/17.
//

#include <gtest/gtest.h>

#include "mdhim.h"

TEST(mdhimOptions, Good) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, false, false), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhimOptions, COMM_NULL) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_NULL, false, false), MDHIM_ERROR);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}
