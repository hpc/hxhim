#include <mpi.h>
#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/private.hpp"
#include "TestDatastore.hpp"

TEST(hxhim, GetStats) {
    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // rank 0 selects a random rank for the stats to be collected at
    int DST_RANK = rand() % hx.p->bootstrap.size;
    ASSERT_EQ(MPI_Bcast(&DST_RANK, 1, MPI_INT, 0, MPI_COMM_WORLD), MPI_SUCCESS);

    // replace datastores with TestDatastore
    for(std::size_t i = 0; i < hx.p->datastores.size(); i++) {
        destruct(hx.p->datastores[i]);
        hx.p->datastores[i] = construct<TestDatastore>(i);
    }

    const std::size_t total_ds = hx.p->bootstrap.size * hx.p->datastores.size();
    long double *put_times = new long double[total_ds]();
    std::size_t *num_puts  = new std::size_t[total_ds]();
    long double *get_times = new long double[total_ds]();
    std::size_t *num_gets  = new std::size_t[total_ds]();

    // PUT some data (actual contents are irrelevant)
    {
        EXPECT_EQ(hxhim::Put(&hx,
                             nullptr, 0,
                             nullptr, 0,
                             HXHIM_BYTE_TYPE,
                             nullptr, 0),
                  HXHIM_SUCCESS);

        // Flush all queued items
        hxhim::Results *put_results = hxhim::FlushPuts(&hx);
        EXPECT_NE(put_results, nullptr);
        hxhim::Results::Destroy(put_results);
    }

    // get stats for 1 PUT
    {
        EXPECT_EQ(hxhim::GetStats(&hx, DST_RANK,
                                  put_times, num_puts,
                                  get_times, num_gets), HXHIM_SUCCESS);

        if (hx.p->bootstrap.rank == DST_RANK) {
            long double total_put_time = 0;
            std::size_t total_num_puts = 0;
            long double total_get_time = 0;
            std::size_t total_num_gets = 0;
            for(std::size_t i = 0; i < total_ds; i++) {
                total_put_time += put_times[i];
                total_num_puts += num_puts[i];
                total_get_time += get_times[i];
                total_num_gets += num_gets[i];
            }

            EXPECT_EQ(total_put_time, hx.p->bootstrap.size * PUT_TIME_DOUBLE);
            EXPECT_EQ(total_num_puts, hx.p->bootstrap.size);
            EXPECT_EQ(total_get_time, 0);
            EXPECT_EQ(total_num_gets, 0);
        }
    }

    // GET some data (actual contents are irrelevant)
    {
        EXPECT_EQ(hxhim::Get(&hx,
                             nullptr, 0,
                             nullptr, 0,
                             HXHIM_BYTE_TYPE),
                  HXHIM_SUCCESS);

        // Flush all queued items
        hxhim::Results *get_results = hxhim::FlushGets(&hx);
        EXPECT_NE(get_results, nullptr);
        hxhim::Results::Destroy(get_results);
    }

    // get stats for 1 PUT and 1 GET
    {
        EXPECT_EQ(hxhim::GetStats(&hx, DST_RANK,
                                  put_times, num_puts,
                                  get_times, num_gets), HXHIM_SUCCESS);

        if (hx.p->bootstrap.rank == DST_RANK) {
            long double total_put_time = 0;
            std::size_t total_num_puts = 0;
            long double total_get_time = 0;
            std::size_t total_num_gets = 0;
            for(std::size_t i = 0; i < total_ds; i++) {
                total_put_time += put_times[i];
                total_num_puts += num_puts[i];
                total_get_time += get_times[i];
                total_num_gets += num_gets[i];
            }

            EXPECT_EQ(total_put_time, hx.p->bootstrap.size * PUT_TIME_DOUBLE);
            EXPECT_EQ(total_num_puts, hx.p->bootstrap.size);
            EXPECT_EQ(total_get_time, hx.p->bootstrap.size * GET_TIME_DOUBLE);
            EXPECT_EQ(total_num_gets, hx.p->bootstrap.size);
        }
    }

    delete [] put_times;
    delete [] num_puts;
    delete [] get_times;
    delete [] num_gets;

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}