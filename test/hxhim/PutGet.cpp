#include <cstdlib>
#include <ctime>
#include <type_traits>

#include <gtest/gtest.h>
#include <mpi.h>

#include "hxhim/hxhim.hpp"
#include "hxhim/private.hpp"
#include "hxhim/backend/InMemory.hpp"
#include "hxhim/config.hpp"

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
    ASSERT_EQ(hxhim_options_set_mpi_bootstrap(&opts, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_backend(&opts, HXHIM_BACKEND_IN_MEMORY, nullptr), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_subject_type(&opts, HXHIM_SPO_BYTE_TYPE), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_predicate_type(&opts, HXHIM_SPO_BYTE_TYPE), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_object_type(&opts, HXHIM_SPO_DOUBLE_TYPE), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_queued_bputs(&opts, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_first_n(&opts, 10), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_options_set_histogram_bucket_gen_method(&opts, TEN_BUCKETS.c_str()), HXHIM_SUCCESS);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // replace backend with in-memory database
    delete hx.p->backend;
    ASSERT_NE(hx.p->backend = new hxhim::backend::InMemory(&hx), nullptr);

    // Add triple for putting
    EXPECT_EQ(hxhim::Put(&hx,
                         (void *)&SUBJECT, sizeof(SUBJECT),
                         (void *)&PREDICATE, sizeof(PREDICATE),
                         (void *)&OBJECT, sizeof(OBJECT)),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *put_results = hxhim::Flush(&hx);
    ASSERT_NE(put_results, nullptr);
    delete put_results;

    // Add subject-predicate to get back
    EXPECT_EQ(hxhim::Get(&hx,
                         (void *)&SUBJECT, sizeof(SUBJECT),
                         (void *)&PREDICATE, sizeof(PREDICATE)),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *get_results = hxhim::Flush(&hx);
    ASSERT_NE(get_results, nullptr);

    // get the results and compare them with the original data
    std::size_t count = 0;
    for(get_results->GoToHead(); get_results->Valid(); get_results->GoToNext()) {
        hxhim::Results::Result *res = get_results->Curr();
        EXPECT_NE(res, nullptr);
        EXPECT_EQ(res->type, HXHIM_RESULT_GET);

        hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(get_results->Curr());
        EXPECT_EQ(* (Subject_t *) get->subject, SUBJECT);
        EXPECT_EQ(* (Predicate_t *) get->predicate, PREDICATE);

        if (std::is_same<float, Object_t>::value) {
            EXPECT_FLOAT_EQ((* (Object_t *) get->object), OBJECT);
        }
        else if (std::is_same<double, Object_t>::value) {
            EXPECT_DOUBLE_EQ((* (Object_t *) get->object), OBJECT);
        }

        count++;
    }

    delete get_results;
    EXPECT_EQ(count, 1);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
