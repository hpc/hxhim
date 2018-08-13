#include <cstdlib>
#include <ctime>
#include <type_traits>

#include <gtest/gtest.h>
#include <mpi.h>

#include "datastore/InMemory.hpp"
#include "hxhim/config.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/private.hpp"

typedef uint64_t Subject_t;
typedef uint64_t Predicate_t;
typedef double   Object_t;

TEST(hxhim, PutGet) {
    srand(time(NULL));

    const Subject_t SUBJECT     = (((Subject_t) rand()) << 32) | rand();
    const Predicate_t PREDICATE = (((Predicate_t) rand()) << 32) | rand();
    const Object_t OBJECT       = (((Object_t) SUBJECT) * ((Object_t) SUBJECT)) / (((Object_t) PREDICATE) * ((Object_t) PREDICATE));

    hxhim_options_t opts;
    ASSERT_EQ(hxhim_options_init(&opts), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD),                    HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastore_in_memory(&opts),                              HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_datastores_per_range_server(&opts, 1),                   HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_hash_name(&opts, RANK.c_str()),                          HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_transport_thallium(&opts, "na+sm"),                      HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1),                                  HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 10),                            HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, TEN_BUCKETS.c_str()), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // Add triple for putting
    EXPECT_EQ(hxhim::PutDouble(&hx,
                               (void *)&SUBJECT, sizeof(SUBJECT),
                               (void *)&PREDICATE, sizeof(PREDICATE),
                               (double *)&OBJECT),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *put_results = hxhim::Flush(&hx);
    ASSERT_NE(put_results, nullptr);

    // Make sure put succeeded
    std::size_t put_count = 0;
    for(put_results->GoToHead(); put_results->Valid(); put_results->GoToNext()) {
        hxhim::Results::Result *res = put_results->Curr();
        ASSERT_NE(res, nullptr);

        EXPECT_EQ(res->GetStatus(), HXHIM_SUCCESS);
        EXPECT_EQ(res->GetType(), HXHIM_RESULT_PUT);

        put_count++;
    }

    delete put_results;
    EXPECT_EQ(put_count, 1);

    // Add subject-predicate to get back
    EXPECT_EQ(hxhim::GetDouble(&hx,
                               (void *)&SUBJECT, sizeof(SUBJECT),
                               (void *)&PREDICATE, sizeof(PREDICATE)),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *get_results = hxhim::Flush(&hx);
    ASSERT_NE(get_results, nullptr);

    // get the results and compare them with the original data
    std::size_t get_count = 0;
    for(get_results->GoToHead(); get_results->Valid(); get_results->GoToNext()) {
        hxhim::Results::Result *res = get_results->Curr();
        ASSERT_NE(res, nullptr);

        ASSERT_EQ(res->GetStatus(), HXHIM_SUCCESS);
        ASSERT_EQ(res->GetType(), HXHIM_RESULT_GET);

        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(get_results->Curr());

        Subject_t *subject = nullptr;
        EXPECT_EQ(get->GetSubject((void **) &subject, nullptr), HXHIM_SUCCESS);
        EXPECT_EQ(*subject, SUBJECT);

        Predicate_t *predicate = nullptr;
        EXPECT_EQ(get->GetPredicate((void **) &predicate, nullptr), HXHIM_SUCCESS);
        EXPECT_EQ(*predicate, PREDICATE);

        Object_t *object = nullptr;
        EXPECT_EQ(get->GetObject((void **) &object, nullptr), HXHIM_SUCCESS);
        ASSERT_NE(object, nullptr);
        if (std::is_same<float, Object_t>::value) {
            EXPECT_FLOAT_EQ(*object, OBJECT);
        }
        else if (std::is_same<double, Object_t>::value) {
            EXPECT_DOUBLE_EQ(*object, OBJECT);
        }

        get_count++;
    }

    delete get_results;
    EXPECT_EQ(get_count, 1);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
