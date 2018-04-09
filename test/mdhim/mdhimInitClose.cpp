//
// Created by bws on 11/7/17.
//

#include <gtest/gtest.h>

#include "mdhim.h"

// Normal usage
TEST(mdhimInitClose, Good) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    EXPECT_EQ(MPI_Barrier(MPI_COMM_WORLD), MPI_SUCCESS);

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

// The communicator provided is MPI_COMM_NULL
TEST(mdhimInit, COMM_NULL) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    mdhim_options_set_comm(&opts, MPI_COMM_NULL);
    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_ERROR);

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

// No md variable
TEST(mdhimInit, NULL_md) {
    mdhim_options_t opts;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    EXPECT_EQ(mdhimInit(NULL, &opts), MDHIM_ERROR);

    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

// No options provided
TEST(mdhimInit, no_opts) {
    mdhim_t md;
    EXPECT_EQ(mdhimInit(&md, nullptr), MDHIM_ERROR);
}

// No md variable
TEST(mdhimClose, no_md) {
    EXPECT_EQ(mdhimClose(nullptr), MDHIM_ERROR);
}
