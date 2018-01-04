//
// Created by bws on 11/7/17.
//

#include "gtest/gtest.h"
#include "mdhim.h"

// Test mdhimInit()
TEST(mdhimTest, InitTest) {

    mdhim_options_t opts;
    mdhim_t mdh;

    int rc = mdhim_options_init(&opts);
    EXPECT_EQ(rc, 0);

    //MPI_Comm appComm = MPI_COMM_WORLD;
    //rc = mdhimInit(&mdh, &opts);
    //EXPECT_EQ(rc, 0);
}

