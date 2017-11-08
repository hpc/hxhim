//
// Created by bws on 11/7/17.
//

#include "gtest/gtest.h"
#include "mdhim.h"

// Test mdhimInit()
TEST(mdhimTest, InitTest) {

    mdhim_options_t opts;
    MPI_Comm appComm = MPI_COMM_WORLD;
    mdhimInit(&appComm, &opts);
}
