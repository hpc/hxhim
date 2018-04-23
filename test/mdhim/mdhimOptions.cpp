//
// Created by bws on 11/7/17.
//

#include <gtest/gtest.h>

#include "mdhim.h"

static const std::size_t ALLOC_SIZE = 128;
static const std::size_t REGIONS = 256;

TEST(mdhimOptions, Good) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    ASSERT_EQ(mdhim_options_init_mpi_transport(&opts, MPI_COMM_WORLD, ALLOC_SIZE, REGIONS), MDHIM_SUCCESS);
    ASSERT_EQ(mdhim_options_init_db(&opts, true), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhimOptions, no_transport) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    ASSERT_EQ(mdhim_options_init_db(&opts, true), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhimOptions, no_db) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    ASSERT_EQ(mdhim_options_init_mpi_transport(&opts, MPI_COMM_WORLD, ALLOC_SIZE, REGIONS), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}
