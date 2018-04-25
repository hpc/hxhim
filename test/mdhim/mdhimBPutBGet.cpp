#include <gtest/gtest.h>
#include <mpi.h>

#include "mdhim.h"
#include "mdhim_private.h"         // needed to access private mdhim members for testing
#include "mdhim_options_private.h" // needed to access private mdhim options members
#include "transport_private.hpp"   // needed to access private return values for testing
#include "MemoryManagers.hpp"

//Constants used across all mdhimBPutBGet tests
typedef int Key_t;
typedef int Value_t;
static const Key_t   MDHIM_BPUT_BGET_PRIMARY_KEY = 13579;
static const Value_t MDHIM_BPUT_BGET_VALUE       = 24680;

static const std::size_t ALLOC_SIZE = 128;
static const std::size_t REGIONS = 256;

//BPut and BGet a key-value pair without secondary indexes
TEST(mdhim, BPutBGet) {
    mdhim_options_t opts;
    mdhim_t md;

    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, true, true), MDHIM_SUCCESS);
    ASSERT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    //Place the key and value into arrays
    void **keys = new void *[1]();
    std::size_t *key_lens = new std::size_t[1]();
    void **values = new void *[1]();
    std::size_t *value_lens = new std::size_t[1]();

    keys[0] = (void *)&MDHIM_BPUT_BGET_PRIMARY_KEY;
    key_lens[0] = sizeof(MDHIM_BPUT_BGET_PRIMARY_KEY);
    values[0] = (void *)&MDHIM_BPUT_BGET_VALUE;
    value_lens[0] = sizeof(MDHIM_BPUT_BGET_VALUE);

    //BPut the key-value pair
    mdhim_brm_t *brm = mdhimBPut(&md, nullptr,
                                 keys, key_lens,
                                 values, value_lens,
                                 1);
    ASSERT_NE(brm, nullptr);
    ASSERT_NE(brm->brm, nullptr);
    EXPECT_EQ(brm->brm->error, MDHIM_SUCCESS);
    mdhim_brm_destroy(brm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //bget value back
    {
        mdhim_bgetrm_t *bgrm = mdhimBGet(&md, md.p->primary_index,
                                         keys, key_lens,
                                         1,
                                         TransportGetMessageOp::GET_EQ);
        ASSERT_NE(bgrm, nullptr);
        EXPECT_EQ(bgrm->bgrm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        EXPECT_EQ(bgrm->bgrm->num_keys, 1);
        EXPECT_EQ(*(Key_t *)bgrm->bgrm->keys[0], MDHIM_BPUT_BGET_PRIMARY_KEY);
        EXPECT_EQ(*(Value_t *)bgrm->bgrm->values[0], MDHIM_BPUT_BGET_VALUE);

        mdhim_bgrm_destroy(bgrm);
    }

    EXPECT_EQ(MPI_Barrier(MPI_COMM_WORLD), MPI_SUCCESS);

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
    EXPECT_EQ(Memory::Pool(ALLOC_SIZE, REGIONS)->used(), 0);

    delete [] keys;
    delete [] key_lens;
    delete [] values;
    delete [] value_lens;
}
