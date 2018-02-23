#include <cstring>
#include <gtest/gtest.h>
#include <mpi.h>

#include "mdhim.h"

// Normal usage
TEST(mdhimClose, Good) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    opts.comm = MPI_COMM_WORLD;

    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
}

// No mdh variable
TEST(mdhimClose, NULL_mdh) {
    EXPECT_EQ(mdhimClose(NULL), MDHIM_ERROR);
}
