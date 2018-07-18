#include <gtest/gtest.h>

#include "mdhim/mdhim.h"
#include "mdhim/options_private.h"

TEST(mdhimOptions, Good) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, true, true), MDHIM_SUCCESS);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhimOptions, COMM_NULL) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_NULL, true, true), MDHIM_ERROR);
    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhimOptions, no_private) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, false, false), MDHIM_SUCCESS);

    ASSERT_NE(opts.p, nullptr);
    ASSERT_NE(opts.p->transport, nullptr);
    ASSERT_NE(opts.p->db, nullptr);

    // delete the private values of opts before destroying opts
    delete opts.p->transport;
    opts.p->transport = nullptr;
    delete opts.p->db;
    opts.p->db = nullptr;
    delete opts.p;
    opts.p = nullptr;

    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhimOptions, no_transport) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, false, false), MDHIM_SUCCESS);

    // opts.p->transport exists, but is empty
    ASSERT_NE(opts.p, nullptr);
    ASSERT_NE(opts.p->transport, nullptr);
    EXPECT_EQ(opts.p->transport->transport_specific, nullptr);
    EXPECT_EQ(opts.p->transport->endpointgroup.size(), 0);

    // remove opts.p->transport before destroying opts
    delete opts.p->transport;
    opts.p->transport = nullptr;

    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
    EXPECT_EQ(opts.p, nullptr);
}

TEST(mdhimOptions, no_db) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, false, false), MDHIM_SUCCESS);

    // opts.p->db exists, but is empty
    EXPECT_EQ(opts.debug_level, 0);
    ASSERT_NE(opts.p, nullptr);
    ASSERT_NE(opts.p->db, nullptr);
    EXPECT_EQ(opts.p->db->path, nullptr);
    EXPECT_EQ(opts.p->db->paths, nullptr);
    EXPECT_EQ(opts.p->db->num_paths, 0);
    EXPECT_EQ(opts.p->db->manifest_path, nullptr);
    EXPECT_EQ(opts.p->db->name, nullptr);
    EXPECT_EQ(opts.p->db->type, 0);
    EXPECT_EQ(opts.p->db->key_type, 0);
    EXPECT_EQ(opts.p->db->value_append, 0);
    EXPECT_EQ(opts.p->db->rserver_factor, 0);
    EXPECT_EQ(opts.p->db->max_recs_per_slice, 0);
    EXPECT_EQ(opts.p->db->num_wthreads, 0);
    EXPECT_EQ(opts.p->db->db_host, nullptr);
    EXPECT_EQ(opts.p->db->dbs_host, nullptr);
    EXPECT_EQ(opts.p->db->db_user, nullptr);
    EXPECT_EQ(opts.p->db->db_upswd, nullptr);
    EXPECT_EQ(opts.p->db->dbs_user, nullptr);
    EXPECT_EQ(opts.p->db->dbs_upswd, nullptr);

    // remove opts.p->db before destroying opts
    delete opts.p->db;
    opts.p->db = nullptr;

    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
    EXPECT_EQ(opts.p, nullptr);
}
