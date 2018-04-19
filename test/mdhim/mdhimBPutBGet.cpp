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

typedef uint32_t sgk_t; // secondary global key type
typedef uint32_t slk_t; // secondary local key type

static const std::size_t ALLOC_SIZE = 128;
static const std::size_t REGIONS = 256;

//BPut and BGet a key-value pair without secondary indexes
TEST(mdhimBPutBGet, no_secondary) {
    mdhim_options_t opts;
    mdhim_t md;

    MPIOptions_t mpi_opts;
    mpi_opts.comm = MPI_COMM_WORLD;
    mpi_opts.alloc_size = ALLOC_SIZE;
    mpi_opts.regions = REGIONS;
    ASSERT_EQ(mdhim_options_init_with_defaults(&opts, MDHIM_TRANSPORT_MPI, &mpi_opts), MDHIM_SUCCESS);
    ASSERT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    //Place the key and value into arrays
    void **keys = new void *[1]();
    int *key_lens = new int[1]();
    void **values = new void *[1]();
    int *value_lens = new int[1]();

    keys[0] = (void *)&MDHIM_BPUT_BGET_PRIMARY_KEY;
    key_lens[0] = sizeof(MDHIM_BPUT_BGET_PRIMARY_KEY);
    values[0] = (void *)&MDHIM_BPUT_BGET_VALUE;
    value_lens[0] = sizeof(MDHIM_BPUT_BGET_VALUE);

    //BPut the key-value pair
    mdhim_brm_t *brm = mdhimBPut(&md,
                                 keys, key_lens,
                                 values, value_lens,
                                 1,
                                 nullptr, nullptr);
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

TEST(mdhimBPutBGet, secondary_global) {
    mdhim_options_t opts;
    mdhim_t md;

    MPIOptions_t mpi_opts;
    mpi_opts.comm = MPI_COMM_WORLD;
    mpi_opts.alloc_size = ALLOC_SIZE;
    mpi_opts.regions = REGIONS;
    ASSERT_EQ(mdhim_options_init_with_defaults(&opts, MDHIM_TRANSPORT_MPI, &mpi_opts), MDHIM_SUCCESS);
    ASSERT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    //Create the secondary global index
    index_t *sg_index = create_global_index(&md, 2, 5, LEVELDB, MDHIM_INT_KEY, nullptr);
    ASSERT_NE(sg_index, nullptr);

    // secondary global key
    sgk_t sgk = 1;
    sgk_t *sgk_ptr = &sgk;
    sgk_t **sgk_arr = &sgk_ptr;
    int sgk_len = sizeof(sgk);
    int *sgk_lens = &sgk_len;
    int sg_num_keys = 1;

    //Create the secondary global info struct
    secondary_bulk_info_t *sg_info = mdhimCreateSecondaryBulkInfo(sg_index,
                                                                  (void ***)&sgk_arr, &sgk_lens,
                                                                  &sg_num_keys, 1, SECONDARY_GLOBAL_INFO);
    ASSERT_NE(sg_info, nullptr);

    //Place the key and value into arrays
    void **keys = new void *[1]();
    int *key_lens = new int[1]();
    void **values = new void *[1]();
    int *value_lens = new int[1]();

    keys[0] = (void *)&MDHIM_BPUT_BGET_PRIMARY_KEY;
    key_lens[0] = sizeof(MDHIM_BPUT_BGET_PRIMARY_KEY);
    values[0] = (void *)&MDHIM_BPUT_BGET_VALUE;
    value_lens[0] = sizeof(MDHIM_BPUT_BGET_VALUE);

    //BPut the key-value pair
    mdhim_brm_t *brm = mdhimBPut(&md,
                                 keys, key_lens,
                                 values, value_lens,
                                 1,
                                 sg_info, nullptr);

    ASSERT_NE(brm, nullptr);
    ASSERT_NE(brm->brm, nullptr);
    EXPECT_EQ(brm->brm->error, MDHIM_SUCCESS);
    mdhim_brm_destroy(brm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //Get key back using the secondary global index
    mdhim_bgetrm_t *sg_ret = nullptr;
    {
        sg_ret = mdhimBGet(&md, sg_index,
                           (void **)sgk_arr, sgk_lens,
                           1,
                           TransportGetMessageOp::GET_EQ);
        ASSERT_NE(sg_ret, nullptr);
        ASSERT_NE(sg_ret->bgrm, nullptr);
        EXPECT_EQ(sg_ret->bgrm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        EXPECT_EQ(sg_ret->bgrm->num_keys, 1);
        EXPECT_EQ(*(sgk_t *)sg_ret->bgrm->keys[0], sgk);
        EXPECT_EQ(*(Key_t *)sg_ret->bgrm->values[0], MDHIM_BPUT_BGET_PRIMARY_KEY);
    }

    //Get value back using the returned key
    {
        mdhim_bgetrm_t *bgrm = mdhimBGet(&md, md.p->primary_index,
                                         sg_ret->bgrm->values, sg_ret->bgrm->value_lens,
                                         1,
                                         TransportGetMessageOp::GET_EQ);
        ASSERT_NE(bgrm, nullptr);
        ASSERT_NE(bgrm->bgrm, nullptr);
        EXPECT_EQ(bgrm->bgrm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        EXPECT_EQ(bgrm->bgrm->num_keys, 1);
        EXPECT_EQ(*(Key_t *)bgrm->bgrm->keys[0], MDHIM_BPUT_BGET_PRIMARY_KEY);
        EXPECT_EQ(*(Value_t *)bgrm->bgrm->values[0], MDHIM_BPUT_BGET_VALUE);

        mdhim_bgrm_destroy(bgrm);
    }

    mdhim_bgrm_destroy(sg_ret);
    mdhimReleaseSecondaryBulkInfo(sg_info);

    EXPECT_EQ(MPI_Barrier(md.mdhim_comm), MPI_SUCCESS);
    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
    EXPECT_EQ(Memory::Pool(ALLOC_SIZE, REGIONS)->used(), 0);

    delete [] keys;
    delete [] key_lens;
    delete [] values;
    delete [] value_lens;
}

TEST(mdhimBPutBGet, secondary_local) {
    mdhim_options_t opts;
    mdhim_t md;

    MPIOptions_t mpi_opts;
    mpi_opts.comm = MPI_COMM_WORLD;
    mpi_opts.alloc_size = ALLOC_SIZE;
    mpi_opts.regions = REGIONS;
    ASSERT_EQ(mdhim_options_init_with_defaults(&opts, MDHIM_TRANSPORT_MPI, &mpi_opts), MDHIM_SUCCESS);
    ASSERT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    //Create the secondary local index
    index_t *sl_index = create_local_index(&md, LEVELDB, MDHIM_INT_KEY, nullptr);
    ASSERT_NE(sl_index, nullptr);

    // secondary local key
    slk_t slk = 2;
    slk_t *slk_ptr = &slk;
    slk_t **slk_arr = &slk_ptr;
    int slk_len = sizeof(slk);
    int *slk_lens = &slk_len;
    int sl_num_keys = 1;

    //Create the secondary local info struct
    secondary_bulk_info_t *sl_info = mdhimCreateSecondaryBulkInfo(sl_index,
                                                                  (void ***)&slk_arr, &slk_lens,
                                                                  &sl_num_keys, 1, SECONDARY_LOCAL_INFO);
    ASSERT_NE(sl_info, nullptr);

    //Place the key and value into arrays
    void **keys = new void *[1]();
    int *key_lens = new int[1]();
    void **values = new void *[1]();
    int *value_lens = new int[1]();

    keys[0] = (void *)&MDHIM_BPUT_BGET_PRIMARY_KEY;
    key_lens[0] = sizeof(MDHIM_BPUT_BGET_PRIMARY_KEY);
    values[0] = (void *)&MDHIM_BPUT_BGET_VALUE;
    value_lens[0] = sizeof(MDHIM_BPUT_BGET_VALUE);

    //BPut the key-value pair
    mdhim_brm_t *brm = mdhimBPut(&md,
                                 keys, key_lens,
                                 values, value_lens,
                                 1,
                                 nullptr, sl_info);

    ASSERT_NE(brm, nullptr);
    ASSERT_NE(brm->brm, nullptr);
    EXPECT_EQ(brm->brm->error, MDHIM_SUCCESS);
    mdhim_brm_destroy(brm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //Get key back using the secondary local index
    mdhim_bgetrm_t *sl_ret = nullptr;
    {
        //Get the stats for the secondary index so the client figures out who to query
        EXPECT_EQ(mdhimStatFlush(&md, sl_index), MDHIM_SUCCESS);

        sl_ret = mdhimBGet(&md, sl_index,
                           (void **)slk_arr, slk_lens,
                           1,
                           TransportGetMessageOp::GET_EQ);
        ASSERT_NE(sl_ret, nullptr);
        ASSERT_NE(sl_ret->bgrm, nullptr);
        EXPECT_EQ(sl_ret->bgrm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        EXPECT_EQ(sl_ret->bgrm->num_keys, 1);
        EXPECT_EQ(*(slk_t *)sl_ret->bgrm->keys[0], slk);
        EXPECT_EQ(*(Key_t *)sl_ret->bgrm->values[0], MDHIM_BPUT_BGET_PRIMARY_KEY);
    }

    //Get value back using the returned key
    {
        mdhim_bgetrm_t *bgrm = mdhimBGet(&md, md.p->primary_index,
                                         sl_ret->bgrm->values, sl_ret->bgrm->value_lens,
                                         1,
                                         TransportGetMessageOp::GET_EQ);
        ASSERT_NE(bgrm, nullptr);
        ASSERT_NE(bgrm->bgrm, nullptr);
        EXPECT_EQ(bgrm->bgrm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        EXPECT_EQ(bgrm->bgrm->num_keys, 1);
        EXPECT_EQ(*(Key_t *)bgrm->bgrm->keys[0], MDHIM_BPUT_BGET_PRIMARY_KEY);
        EXPECT_EQ(*(Value_t *)bgrm->bgrm->values[0], MDHIM_BPUT_BGET_VALUE);

        mdhim_bgrm_destroy(bgrm);
    }

    mdhim_bgrm_destroy(sl_ret);
    mdhimReleaseSecondaryBulkInfo(sl_info);

    EXPECT_EQ(MPI_Barrier(md.mdhim_comm), MPI_SUCCESS);
    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
    EXPECT_EQ(Memory::Pool(ALLOC_SIZE, REGIONS)->used(), 0);

    delete [] keys;
    delete [] key_lens;
    delete [] values;
    delete [] value_lens;
}

TEST(mdhimBPutBGet, secondary_global_and_local) {
    mdhim_options_t opts;
    mdhim_t md;

    MPIOptions_t mpi_opts;
    mpi_opts.comm = MPI_COMM_WORLD;
    mpi_opts.alloc_size = ALLOC_SIZE;
    mpi_opts.regions = REGIONS;
    ASSERT_EQ(mdhim_options_init_with_defaults(&opts, MDHIM_TRANSPORT_MPI, &mpi_opts), MDHIM_SUCCESS);
    ASSERT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    //Create the secondary global index
    index_t *sg_index = create_global_index(&md, 2, 5, LEVELDB, MDHIM_INT_KEY, nullptr);
    ASSERT_NE(sg_index, nullptr);

    // secondary global key
    sgk_t sgk = 1;
    sgk_t *sgk_ptr = &sgk;
    sgk_t **sgk_arr = &sgk_ptr;
    int sgk_len = sizeof(sgk);
    int *sgk_lens = &sgk_len;
    int sg_num_keys = 1;

    //Create the secondary global info struct
    secondary_bulk_info_t *sg_info = mdhimCreateSecondaryBulkInfo(sg_index,
                                                                  (void ***)&sgk_arr, &sgk_lens,
                                                                  &sg_num_keys, 1, SECONDARY_GLOBAL_INFO);
    ASSERT_NE(sg_info, nullptr);

    //Create the secondary local index
    index_t *sl_index = create_local_index(&md, LEVELDB, MDHIM_INT_KEY, nullptr);
    ASSERT_NE(sl_index, nullptr);

    // secondary local key
    slk_t slk = 2;
    slk_t *slk_ptr = &slk;
    slk_t **slk_arr = &slk_ptr;
    int slk_len = sizeof(slk);
    int *slk_lens = &slk_len;
    int sl_num_keys = 1;

    //Create the secondary local info struct
    secondary_bulk_info_t *sl_info = mdhimCreateSecondaryBulkInfo(sl_index,
                                                                  (void ***)&slk_arr, &slk_lens,
                                                                  &sl_num_keys, 1, SECONDARY_LOCAL_INFO);
    ASSERT_NE(sl_info, nullptr);

    //Place the key and value into arrays
    void **keys = new void *[1]();
    int *key_lens = new int[1]();
    void **values = new void *[1]();
    int *value_lens = new int[1]();

    keys[0] = (void *)&MDHIM_BPUT_BGET_PRIMARY_KEY;
    key_lens[0] = sizeof(MDHIM_BPUT_BGET_PRIMARY_KEY);
    values[0] = (void *)&MDHIM_BPUT_BGET_VALUE;
    value_lens[0] = sizeof(MDHIM_BPUT_BGET_VALUE);

    //BPut the key-value pair
    mdhim_brm_t *brm = mdhimBPut(&md,
                                 keys, key_lens,
                                 values, value_lens,
                                 1,
                                 sg_info, sl_info);
    ASSERT_NE(brm, nullptr);
    ASSERT_NE(brm->brm, nullptr);
    EXPECT_EQ(brm->brm->error, MDHIM_SUCCESS);
    mdhim_brm_destroy(brm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //get value back
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

    {
        //Get key back using the secondary global index
        mdhim_bgetrm_t *sg_ret = nullptr;
        {
            sg_ret = mdhimBGet(&md, sg_index,
                               (void **)sgk_arr, sgk_lens,
                               1,
                               TransportGetMessageOp::GET_EQ);
            ASSERT_NE(sg_ret, nullptr);
            ASSERT_NE(sg_ret->bgrm, nullptr);
            EXPECT_EQ(sg_ret->bgrm->error, MDHIM_SUCCESS);

            //Make sure value gotten back is correct
            EXPECT_EQ(sg_ret->bgrm->num_keys, 1);
            EXPECT_EQ(*(sgk_t *)sg_ret->bgrm->keys[0], sgk);
            EXPECT_EQ(*(Key_t *)sg_ret->bgrm->values[0], MDHIM_BPUT_BGET_PRIMARY_KEY);
        }

        //Get value back using the returned key
        {
            mdhim_bgetrm_t *bgrm = mdhimBGet(&md, md.p->primary_index,
                                             sg_ret->bgrm->values, sg_ret->bgrm->value_lens,
                                             1,
                                             TransportGetMessageOp::GET_EQ);
            ASSERT_NE(bgrm, nullptr);
            ASSERT_NE(bgrm->bgrm, nullptr);
            EXPECT_EQ(bgrm->bgrm->error, MDHIM_SUCCESS);

            //Make sure value gotten back is correct
            EXPECT_EQ(bgrm->bgrm->num_keys, 1);
            EXPECT_EQ(*(Key_t *)bgrm->bgrm->keys[0], MDHIM_BPUT_BGET_PRIMARY_KEY);
            EXPECT_EQ(*(Value_t *)bgrm->bgrm->values[0], MDHIM_BPUT_BGET_VALUE);

            mdhim_bgrm_destroy(bgrm);
        }

        mdhim_bgrm_destroy(sg_ret);
        mdhimReleaseSecondaryBulkInfo(sg_info);
    }

    //Get key back using the secondary local index
    {
        //Get the primary key
        mdhim_bgetrm_t *sl_ret = nullptr;
        {
            //Get the stats for the secondary index so the client figures out who to query
            EXPECT_EQ(mdhimStatFlush(&md, sl_index), MDHIM_SUCCESS);

            sl_ret = mdhimBGet(&md, sl_index,
                               (void **)slk_arr, slk_lens,
                               1,
                               TransportGetMessageOp::GET_EQ);
            ASSERT_NE(sl_ret, nullptr);
            ASSERT_NE(sl_ret->bgrm, nullptr);
            EXPECT_EQ(sl_ret->bgrm->error, MDHIM_SUCCESS);

            //Make sure value gotten back is correct
            EXPECT_EQ(sl_ret->bgrm->num_keys, 1);
            EXPECT_EQ(*(slk_t *)sl_ret->bgrm->keys[0], slk);
            EXPECT_EQ(*(Key_t *)sl_ret->bgrm->values[0], MDHIM_BPUT_BGET_PRIMARY_KEY);
        }

        //Get value back using the returned key
        {
            mdhim_bgetrm_t *bgrm = mdhimBGet(&md, md.p->primary_index,
                                             sl_ret->bgrm->values, sl_ret->bgrm->value_lens,
                                             1,
                                             TransportGetMessageOp::GET_EQ);
            ASSERT_NE(bgrm, nullptr);
            ASSERT_NE(bgrm->bgrm, nullptr);
            EXPECT_EQ(bgrm->bgrm->error, MDHIM_SUCCESS);

            //Make sure value gotten back is correct
            EXPECT_EQ(bgrm->bgrm->num_keys, 1);
            EXPECT_EQ(*(Key_t *)bgrm->bgrm->keys[0], MDHIM_BPUT_BGET_PRIMARY_KEY);
            EXPECT_EQ(*(Value_t *)bgrm->bgrm->values[0], MDHIM_BPUT_BGET_VALUE);

            mdhim_bgrm_destroy(bgrm);
        }

        mdhim_bgrm_destroy(sl_ret);
        mdhimReleaseSecondaryBulkInfo(sl_info);
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
