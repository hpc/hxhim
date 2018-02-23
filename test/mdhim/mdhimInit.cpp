//
// Created by bws on 11/7/17.
//

#include <cstring>
#include <gtest/gtest.h>
#include <mpi.h>

#include "mdhim.h"

// Normal usage
TEST(mdhimInit, Good) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    opts.comm = MPI_COMM_WORLD;

    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);
}

// Communicator has not been initialized
// This is undefined behavior and MPI is likely to just crash
// TEST(mdhimInit, No_Comm) {
//     mdhim_options_t opts;
//     mdhim_t md;

//     EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
//     EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_ERROR);
// }

// The communicator provided is MPI_COMM_NULL
TEST(mdhimInit, NULL_Comm) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    opts.comm = MPI_COMM_NULL;

    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_ERROR);
}

// No mdh variable
TEST(mdhimInit, NULL_mdh) {
    mdhim_options_t opts;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    opts.comm = MPI_COMM_WORLD;

    EXPECT_EQ(mdhimInit(NULL, &opts), MDHIM_ERROR);
}

// No options provided
TEST(mdhimInit, NULL_opts) {
    mdhim_t md;
    EXPECT_EQ(mdhimInit(&md, NULL), MDHIM_ERROR);
}
