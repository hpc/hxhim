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

typedef uint32_t sgk_t; // secondary global key type
typedef uint32_t slk_t; // secondary local key type

static const std::size_t ALLOC_SIZE = 128;
static const std::size_t REGIONS = 256;

//Put and Get a key-value pair without secondary indexes
TEST(mdhimPutGet, no_secondary) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    MPIOptions_t *mpi_opts = new MPIOptions_t();
    mpi_opts->comm = MPI_COMM_WORLD;
    mpi_opts->alloc_size = ALLOC_SIZE;
    mpi_opts->regions = REGIONS;
    mdhim_options_set_transport(&opts, MDHIM_TRANSPORT_MPI, mpi_opts);
    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    //Put the key-value pair
    mdhim_brm_t *brm = mdhimPut(&md,
                                (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE),
                                NULL, NULL);
    ASSERT_NE(brm, nullptr);
    ASSERT_NE(brm->p, nullptr);
    ASSERT_NE(brm->p->brm, nullptr);
    EXPECT_EQ(brm->p->brm->error, MDHIM_SUCCESS);
    mdhim_brm_destroy(brm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //Get value back
    {
        mdhim_getrm_t *grm = mdhimGet(&md, md.p->primary_index,
                                      (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                      TransportGetMessageOp::GET_EQ);
        ASSERT_NE(grm, nullptr);
        EXPECT_EQ(grm->p->grm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        EXPECT_EQ(*(Key_t *)grm->p->grm->key, MDHIM_PUT_GET_PRIMARY_KEY);
        EXPECT_EQ(*(Value_t *)grm->p->grm->value, MDHIM_PUT_GET_VALUE);

        mdhim_grm_destroy(grm);
    }

    EXPECT_EQ(MPI_Barrier(MPI_COMM_WORLD), MPI_SUCCESS);

    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
    EXPECT_EQ(Memory::Pool(ALLOC_SIZE, REGIONS)->used(), 0);
}

TEST(mdhimPutGet, secondary_global) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    MPIOptions_t *mpi_opts = new MPIOptions_t();
    mpi_opts->comm = MPI_COMM_WORLD;
    mpi_opts->alloc_size = ALLOC_SIZE;
    mpi_opts->regions = REGIONS;
    mdhim_options_set_transport(&opts, MDHIM_TRANSPORT_MPI, mpi_opts);
    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    // secondary global key
    sgk_t sgk = 1;
    sgk_t *sgk_ptr = &sgk;
    int sgk_len = sizeof(sgk);

    //Create the secondary remote index
    struct index *sg_index = create_global_index(&md, 2, 5, LEVELDB, MDHIM_INT_KEY, NULL);
    ASSERT_NE(sg_index, nullptr);

    //Create the secondary info struct
    struct secondary_info *sg_info = mdhimCreateSecondaryInfo(sg_index,
                                                              (void **)&sgk_ptr, &sgk_len,
                                                              1, SECONDARY_GLOBAL_INFO);
    ASSERT_NE(sg_info, nullptr);

    //Put the key-value pair
    mdhim_brm_t *brm = mdhimPut(&md,
                                (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE),
                                sg_info, NULL);

    ASSERT_NE(brm, nullptr);
    ASSERT_NE(brm->p, nullptr);
    ASSERT_NE(brm->p->brm, nullptr);
    EXPECT_EQ(brm->p->brm->error, MDHIM_SUCCESS);
    mdhim_brm_destroy(brm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //Get key back using the secondary global index
    mdhim_getrm_t *sg_ret = nullptr;
    {
        sg_ret = mdhimGet(&md, sg_index, (void *)sgk_ptr,
                          sgk_len, TransportGetMessageOp::GET_EQ);
        ASSERT_NE(sg_ret, nullptr);
        ASSERT_NE(sg_ret->p, nullptr);
        ASSERT_NE(sg_ret->p->grm, nullptr);
        EXPECT_EQ(sg_ret->p->grm->error, MDHIM_SUCCESS);

        // EXPECT_EQ(sg_ret->p->grm->num_keys, 1);
        EXPECT_EQ(*(sgk_t *)sg_ret->p->grm->key, sgk);
        EXPECT_EQ(*(Key_t *)sg_ret->p->grm->value, MDHIM_PUT_GET_PRIMARY_KEY);
    }

    //Get value back using the returned key
    {
        mdhim_getrm_t *grm = mdhimGet(&md, md.p->primary_index,
                                      sg_ret->p->grm->value, sg_ret->p->grm->value_len,
                                      TransportGetMessageOp::GET_EQ);
        ASSERT_NE(grm, nullptr);
        ASSERT_NE(grm->p, nullptr);
        ASSERT_NE(grm->p->grm, nullptr);
        EXPECT_EQ(grm->p->grm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        // EXPECT_EQ(grm->p->grm->num_keys, 1);
        EXPECT_EQ(*(Key_t *)grm->p->grm->key, MDHIM_PUT_GET_PRIMARY_KEY);
        EXPECT_EQ(*(Value_t *)grm->p->grm->value, MDHIM_PUT_GET_VALUE);

        mdhim_grm_destroy(grm);
    }

    mdhim_grm_destroy(sg_ret);
    mdhimReleaseSecondaryInfo(sg_info);

    EXPECT_EQ(MPI_Barrier(md.mdhim_comm), MPI_SUCCESS);
    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
    EXPECT_EQ(Memory::Pool(ALLOC_SIZE, REGIONS)->used(), 0);
}

TEST(mdhimPutGet, secondary_local) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    MPIOptions_t *mpi_opts = new MPIOptions_t();
    mpi_opts->comm = MPI_COMM_WORLD;
    mpi_opts->alloc_size = ALLOC_SIZE;
    mpi_opts->regions = REGIONS;
    mdhim_options_set_transport(&opts, MDHIM_TRANSPORT_MPI, mpi_opts);
    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    // secondary local key
    slk_t slk = 2;
    slk_t *slk_ptr = &slk;
    int slk_len = sizeof(slk);

    //Create the secondary local index
    struct index *sl_index = create_local_index(&md, LEVELDB, MDHIM_INT_KEY, "local");
    ASSERT_NE(sl_index, nullptr);

    //Create the secondary info struct
    struct secondary_info *sl_info = mdhimCreateSecondaryInfo(sl_index,
                                                              (void **)&slk_ptr, &slk_len,
                                                              1, SECONDARY_GLOBAL_INFO);
    ASSERT_NE(sl_info, nullptr);

    mdhim_brm_t *brm = mdhimPut(&md,
                                (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE),
                                nullptr, sl_info);

    ASSERT_NE(brm, nullptr);
    ASSERT_NE(brm->p, nullptr);
    ASSERT_NE(brm->p->brm, nullptr);
    EXPECT_EQ(brm->p->brm->error, MDHIM_SUCCESS);
    mdhim_brm_destroy(brm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //Get the primary key from the secondary local index
    mdhim_getrm_t *sl_ret = nullptr;
    {
        //Get the stats for the secondary index so the client figures out who to query
        EXPECT_EQ(mdhimStatFlush(&md, sl_index), MDHIM_SUCCESS);

        sl_ret = mdhimGet(&md, sl_index, slk_ptr,
                          slk_len, TransportGetMessageOp::GET_PRIMARY_EQ);
        ASSERT_NE(sl_ret, nullptr);
        ASSERT_NE(sl_ret->p, nullptr);
        ASSERT_NE(sl_ret->p->grm, nullptr);
        EXPECT_EQ(sl_ret->p->grm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        // EXPECT_EQ(sl_ret->p->grm->num_keys, 1);
        EXPECT_EQ(*(Key_t *)sl_ret->p->grm->key, slk);
        EXPECT_EQ(*(Value_t *)sl_ret->p->grm->value, MDHIM_PUT_GET_PRIMARY_KEY);
    }

    //Get value back using the returned key
    {
        mdhim_getrm_t *grm = mdhimGet(&md, md.p->primary_index,
                                      sl_ret->p->grm->value, sl_ret->p->grm->value_len,
                                      TransportGetMessageOp::GET_EQ);
        ASSERT_NE(grm, nullptr);
        ASSERT_NE(grm->p, nullptr);
        ASSERT_NE(grm->p->grm, nullptr);
        EXPECT_EQ(grm->p->grm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        // EXPECT_EQ(grm->p->grm->num_keys, 1);
        EXPECT_EQ(*(slk_t *)grm->p->grm->key, MDHIM_PUT_GET_PRIMARY_KEY);
        EXPECT_EQ(*(Key_t *)grm->p->grm->value, MDHIM_PUT_GET_VALUE);

        mdhim_grm_destroy(grm);
    }
    mdhim_grm_destroy(sl_ret);
    mdhimReleaseSecondaryInfo(sl_info);

    EXPECT_EQ(MPI_Barrier(md.mdhim_comm), MPI_SUCCESS);
    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
    EXPECT_EQ(Memory::Pool(ALLOC_SIZE, REGIONS)->used(), 0);
}

TEST(mdhimPutGet, secondary_global_and_local) {
    mdhim_options_t opts;
    mdhim_t md;

    EXPECT_EQ(mdhim_options_init(&opts), MDHIM_SUCCESS);
    MPIOptions_t *mpi_opts = new MPIOptions_t();
    mpi_opts->comm = MPI_COMM_WORLD;
    mpi_opts->alloc_size = ALLOC_SIZE;
    mpi_opts->regions = REGIONS;
    mdhim_options_set_transport(&opts, MDHIM_TRANSPORT_MPI, mpi_opts);
    EXPECT_EQ(mdhimInit(&md, &opts), MDHIM_SUCCESS);

    // secondary global key
    sgk_t sgk = 1;
    sgk_t *sgk_ptr = &sgk;
    int sgk_len = sizeof(sgk);

    //Create the secondary remote index
    struct index *sg_index = create_global_index(&md, 2, 5, LEVELDB, MDHIM_INT_KEY, NULL);
    ASSERT_NE(sg_index, nullptr);

    //Create the secondary info struct
    struct secondary_info *sg_info = mdhimCreateSecondaryInfo(sg_index,
                                                              (void **)&sgk_ptr, &sgk_len,
                                                              1, SECONDARY_GLOBAL_INFO);
    ASSERT_NE(sg_info, nullptr);

    // secondary local key
    slk_t slk = 2;
    slk_t *slk_ptr = &slk;
    int slk_len = sizeof(slk);

    //Create the secondary local index
    struct index *sl_index = create_local_index(&md, LEVELDB, MDHIM_INT_KEY, "local");
    ASSERT_NE(sl_index, nullptr);

    //Create the secondary info struct
    struct secondary_info *sl_info = mdhimCreateSecondaryInfo(sl_index,
                                                              (void **)&slk_ptr, &slk_len,
                                                              1, SECONDARY_GLOBAL_INFO);
    ASSERT_NE(sl_info, nullptr);

    //Put the key-value pair
    mdhim_brm_t *brm = mdhimPut(&md,
                                (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                (void *)&MDHIM_PUT_GET_VALUE, sizeof(MDHIM_PUT_GET_VALUE),
                                sg_info, sl_info);
    ASSERT_NE(brm, nullptr);
    ASSERT_NE(brm->p, nullptr);
    ASSERT_NE(brm->p->brm, nullptr);
    EXPECT_EQ(brm->p->brm->error, MDHIM_SUCCESS);
    mdhim_brm_destroy(brm);

    //Commit changes
    EXPECT_EQ(mdhimCommit(&md, md.p->primary_index), MDHIM_SUCCESS);

    //Get value back using primary key
    {
        mdhim_getrm_t *grm = mdhimGet(&md, md.p->primary_index,
                                      (void *)&MDHIM_PUT_GET_PRIMARY_KEY, sizeof(MDHIM_PUT_GET_PRIMARY_KEY),
                                      TransportGetMessageOp::GET_EQ);
        ASSERT_NE(grm, nullptr);
        ASSERT_NE(grm->p, nullptr);
        ASSERT_NE(grm->p->grm, nullptr);
        EXPECT_EQ(grm->p->grm->error, MDHIM_SUCCESS);

        //Make sure value gotten back is correct
        // EXPECT_EQ(grm->p->grm->num_keys, 1);
        EXPECT_EQ(*(Key_t *)grm->p->grm->key, MDHIM_PUT_GET_PRIMARY_KEY);
        EXPECT_EQ(*(Value_t *)grm->p->grm->value, MDHIM_PUT_GET_VALUE);

        mdhim_grm_destroy(grm);
    }

    // secondary global index
    {
        //Get the primary key
        mdhim_getrm_t *sg_ret = nullptr;
        {
            sg_ret = mdhimGet(&md, sg_index, (void *)sgk_ptr,
                              sgk_len, TransportGetMessageOp::GET_EQ);
            ASSERT_NE(sg_ret, nullptr);
            ASSERT_NE(sg_ret->p, nullptr);
            ASSERT_NE(sg_ret->p->grm, nullptr);
            EXPECT_EQ(sg_ret->p->grm->error, MDHIM_SUCCESS);

            // EXPECT_EQ(sg_ret->p->grm->num_keys, 1);
            EXPECT_EQ(*(sgk_t *)sg_ret->p->grm->key, sgk);
            EXPECT_EQ(*(Key_t *)sg_ret->p->grm->value, MDHIM_PUT_GET_PRIMARY_KEY);
        }

        //Get value back using the returned key
        {
            mdhim_getrm_t *grm = mdhimGet(&md, md.p->primary_index,
                                          sg_ret->p->grm->value, sg_ret->p->grm->value_len,
                                          TransportGetMessageOp::GET_EQ);
            ASSERT_NE(grm, nullptr);
            ASSERT_NE(grm->p, nullptr);
            ASSERT_NE(grm->p->grm, nullptr);
            EXPECT_EQ(grm->p->grm->error, MDHIM_SUCCESS);

            //Make sure value gotten back is correct
            // EXPECT_EQ(grm->p->grm->num_keys, 1);
            EXPECT_EQ(*(Key_t *)grm->p->grm->key, MDHIM_PUT_GET_PRIMARY_KEY);
            EXPECT_EQ(*(Value_t *)grm->p->grm->value, MDHIM_PUT_GET_VALUE);

            mdhim_grm_destroy(grm);
        }

        mdhim_grm_destroy(sg_ret);
        mdhimReleaseSecondaryInfo(sg_info);
    }

    // secondary local index
    {
        //Get the primary key
        mdhim_getrm_t *sl_ret = nullptr;
        {
            //Get the stats for the secondary index so the client figures out who to query
            EXPECT_EQ(mdhimStatFlush(&md, sl_index), MDHIM_SUCCESS);

            sl_ret = mdhimGet(&md, sl_index, slk_ptr,
                              slk_len, TransportGetMessageOp::GET_PRIMARY_EQ);
            ASSERT_NE(sl_ret, nullptr);
            ASSERT_NE(sl_ret->p, nullptr);
            ASSERT_NE(sl_ret->p->grm, nullptr);
            EXPECT_EQ(sl_ret->p->grm->error, MDHIM_SUCCESS);

            //Make sure value gotten back is correct
            // EXPECT_EQ(sl_ret->p->grm->num_keys, 1);
            EXPECT_EQ(*(slk_t *)sl_ret->p->grm->key, slk);
            EXPECT_EQ(*(Key_t *)sl_ret->p->grm->value, MDHIM_PUT_GET_PRIMARY_KEY);
        }

        //Get value back using the returned key
        {
            mdhim_getrm_t *grm = mdhimGet(&md, md.p->primary_index,
                                          sl_ret->p->grm->value, sl_ret->p->grm->value_len,
                                          TransportGetMessageOp::GET_EQ);
            ASSERT_NE(grm, nullptr);
            ASSERT_NE(grm->p, nullptr);
            ASSERT_NE(grm->p->grm, nullptr);
            EXPECT_EQ(grm->p->grm->error, MDHIM_SUCCESS);

            //Make sure value gotten back is correct
            // EXPECT_EQ(grm->p->grm->num_keys, 1);
            EXPECT_EQ(*(Key_t *)grm->p->grm->key, MDHIM_PUT_GET_PRIMARY_KEY);
            EXPECT_EQ(*(Value_t *)grm->p->grm->value, MDHIM_PUT_GET_VALUE);

            mdhim_grm_destroy(grm);
        }

        mdhim_grm_destroy(sl_ret);
        mdhimReleaseSecondaryInfo(sl_info);
    }

    EXPECT_EQ(MPI_Barrier(md.mdhim_comm), MPI_SUCCESS);
    EXPECT_EQ(mdhimClose(&md), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
    EXPECT_EQ(Memory::Pool(ALLOC_SIZE, REGIONS)->used(), 0);
}
