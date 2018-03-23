//
// Created by bws on 11/7/17.
//

#include <gtest/gtest.h>
#include <mpi.h>

#include "MPIEndpoint.hpp"
#include "fill_db_opts.h"
#include "mdhim.h"
#include "mdhim_private.h"       // needed for access to the endpoint

// Normal usage
TEST(mdhimInitClose, Good) {
    mdhim_options_t opts;
    mdhim_t md;
    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    fill_db_opts(opts);

    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);
    EXPECT_EQ(MPI_Barrier(MPI_COMM_WORLD), MPI_SUCCESS);
    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
}

// The communicator provided is MPI_COMM_NULL
TEST(mdhimInit, NULL_Transport) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    fill_db_opts(opts);
    opts.comm = MPI_COMM_NULL;

    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_ERROR);
}

// No mdh variable
TEST(mdhimInit, NULL_mdh) {
    mdhim_options_t opts;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    fill_db_opts(opts);

    EXPECT_EQ(mdhimInit(NULL, &opts), MDHIM_ERROR);
}

// No options provided
TEST(mdhimInit, NULL_opts) {
    mdhim_t md;
    EXPECT_EQ(mdhimInit(&md, NULL), MDHIM_ERROR);
}

// No mdh variable
TEST(mdhimClose, NULL_mdh) {
    EXPECT_EQ(mdhimClose(NULL), MDHIM_ERROR);
}
