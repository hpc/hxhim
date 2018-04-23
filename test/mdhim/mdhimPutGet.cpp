#include <gtest/gtest.h>
#include <mpi.h>

#include "mdhim.h"
#include "mdhim_private.h"         // needed to access private mdhim members for testing
#include "mdhim_options_private.h" // needed to access private mdhim options members
#include "transport_private.hpp"   // needed to access private return values for testing
#include "MemoryManagers.hpp"

//Constants used across all mdhimPutGet tests
typedef int Key_t;
typedef int Value_t;
static const Key_t   MDHIM_PUT_GET_PRIMARY_KEY = 13579;
static const Value_t MDHIM_PUT_GET_VALUE       = 24680;

static const std::size_t ALLOC_SIZE = 128;
static const std::size_t REGIONS = 256;

//Put and Get a key-value pair without secondary indexes
TEST(mdhim, PutGet) {
    mdhim_options_t opts;
    mdhim_t md;

    MPIOptions_t mpi_opts;
    mpi_opts.comm = MPI_COMM_WORLD;
    mpi_opts.alloc_size = ALLOC_SIZE;
    mpi_opts.regions = REGIONS;
    ASSERT_EQ(mdhim_options_init_with_defaults(&opts, MDHIM_TRANSPORT_MPI, &mpi_opts), MDHIM_SUCCESS);
    ASSERT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    //Put the key-value pair
    mdhim_rm_t *rm = mdhimPut(&md, nullptr,
                              (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                              (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE));
    ASSERT_NE(rm, nullptr);
    ASSERT_NE(rm->rm, nullptr);
    EXPECT_EQ(rm->rm->error, MDHIM_SUCCESS);
    mdhim_rm_destroy(rm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //Get value back
    {
        mdhim_getrm_t *grm = mdhimGet(&md, md.p->primary_index,
                                      (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                      TransportGetMessageOp::GET_EQ);
        ASSERT_NE(grm, nullptr);
        EXPECT_EQ(grm->grm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        EXPECT_EQ(*(Key_t *)grm->grm->key, MDHIM_PUT_GET_PRIMARY_KEY);
        EXPECT_EQ(*(Value_t *)grm->grm->value, MDHIM_PUT_GET_VALUE);

        mdhim_grm_destroy(grm);
    }

    EXPECT_EQ(MPI_Barrier(MPI_COMM_WORLD), MPI_SUCCESS);

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
    EXPECT_EQ(Memory::Pool(ALLOC_SIZE, REGIONS)->used(), 0);
}
