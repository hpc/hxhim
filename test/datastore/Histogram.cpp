#include <gtest/gtest.h>

#include "TestDatastore.hpp"
#include "utils/elen.hpp"

const std::size_t FIRST_N = 5;

void check_nothing_happend(Histogram::Histogram *hist) {
    std::size_t first_n = 0;
    double *cache = nullptr;
    std::size_t cache_size = 0;

    EXPECT_EQ(hist->get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
    EXPECT_EQ(first_n, FIRST_N);
    EXPECT_NE(cache, nullptr);
    EXPECT_EQ(cache_size, 0);
    EXPECT_EQ(hist->get(nullptr, nullptr, nullptr), HISTOGRAM_ERROR);
}

TEST(datastore, Histogram) {
    Histogram::Histogram *hist = construct<Histogram::Histogram>(
        Histogram::Config{FIRST_N,
                          [](const double *, const size_t,
                             double **buckets, size_t *size,
                             void *) -> int {
                              if (!buckets || !size) {
                                  return HISTOGRAM_ERROR;
                              }

                              *size = 1;
                              *buckets = alloc_array<double>(*size);
                              (*buckets)[0] = 0;

                              return HISTOGRAM_SUCCESS;
                          },
                          nullptr}
        );
    TestDatastore ds(-1, hist);

    // before any events
    check_nothing_happend(hist);

    // GET should not update the histogram
    {
        {
            Transport::Request::BGet flt(1);
            flt.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            flt.predicates[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            flt.object_types[0] = hxhim_data_t::HXHIM_DATA_FLOAT;
            flt.count = 1;
            ds.operate(&flt);

            check_nothing_happend(hist);
        }

        {
            Transport::Request::BGet dbl(1);
            dbl.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            dbl.predicates[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            dbl.object_types[0] = hxhim_data_t::HXHIM_DATA_DOUBLE;
            dbl.count = 1;
            ds.operate(&dbl);

            check_nothing_happend(hist);
        }
    }

    // GETOP should not update the histogram
    {
        {
            Transport::Request::BGetOp flt(1);
            flt.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            flt.predicates[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            flt.object_types[0] = hxhim_data_t::HXHIM_DATA_FLOAT;
            flt.count = 1;
            ds.operate(&flt);

            check_nothing_happend(hist);
        }

        {
            Transport::Request::BGetOp dbl(1);
            dbl.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            dbl.predicates[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            dbl.object_types[0] = hxhim_data_t::HXHIM_DATA_DOUBLE;
            dbl.count = 1;
            ds.operate(&dbl);

            check_nothing_happend(hist);
        }
    }

    // DELETE should not update the histogram
    {
        {
            Transport::Request::BDelete flt(1);
            flt.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            flt.predicates[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            flt.count = 1;
            ds.operate(&flt);

            check_nothing_happend(hist);
        }

        {
            Transport::Request::BDelete dbl(1);
            dbl.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            dbl.predicates[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            dbl.count = 1;
            ds.operate(&dbl);

            check_nothing_happend(hist);
        }
    }

    // PUT some values
    {
        std::size_t first_n = 0;
        double *cache = nullptr;
        std::size_t cache_size = 0;

        {
            const float object = 0;
            const std::string object_str = elen::encode::floating_point<float>(object);

            Transport::Request::BPut flt(1);
            flt.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            flt.predicates[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            flt.objects[0] = ReferenceBlob((void *) object_str.data(), object_str.size(),
                                           hxhim_data_t::HXHIM_DATA_FLOAT);
            flt.count = 1;
            destruct(ds.operate(&flt));

            EXPECT_EQ(hist->get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
            EXPECT_EQ(first_n, FIRST_N);
            EXPECT_NE(cache, nullptr);
            EXPECT_EQ(cache_size, 1);
            EXPECT_EQ(hist->get(nullptr, nullptr, nullptr), HISTOGRAM_ERROR);
        }

        for(std::size_t i = 1; i < FIRST_N; i++) {
            EXPECT_EQ(hist->get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
            EXPECT_EQ(first_n, FIRST_N);
            EXPECT_NE(cache, nullptr);
            EXPECT_EQ(cache_size, i);
            EXPECT_EQ(hist->get(nullptr, nullptr, nullptr), HISTOGRAM_ERROR);

            const double object = 0;
            const std::string object_str = elen::encode::floating_point<double>(object);

            Transport::Request::BPut dbl(1);
            dbl.subjects[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            dbl.predicates[0] = ReferenceBlob(nullptr, 0, hxhim_data_t::HXHIM_DATA_POINTER);
            dbl.objects[0] = ReferenceBlob((void *) object_str.data(), object_str.size(),
                                           hxhim_data_t::HXHIM_DATA_DOUBLE);
            dbl.count = 1;
            destruct(ds.operate(&dbl));
        }

        EXPECT_EQ(hist->get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);
        EXPECT_EQ(first_n, FIRST_N);
        EXPECT_NE(cache, nullptr);
        EXPECT_EQ(cache_size, FIRST_N);

        double *buckets = nullptr;
        std::size_t *counts = nullptr;
        std::size_t size = 0;
        EXPECT_EQ(hist->get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);
        EXPECT_NE(buckets, nullptr);
        EXPECT_NE(counts, nullptr);
        EXPECT_EQ(size, 1);
        EXPECT_EQ(buckets[0], 0);
        EXPECT_EQ(counts[0], FIRST_N);
    }
}
