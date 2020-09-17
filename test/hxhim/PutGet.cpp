#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

typedef uint64_t Subject_t;
typedef uint64_t Predicate_t;
typedef double   Object_t;

TEST(hxhim, PutGet) {
    const Subject_t   SUBJECT   = (((Subject_t)   rand()) << 32) | rand();
    const Predicate_t PREDICATE = (((Predicate_t) rand()) << 32) | rand();
    const Object_t    OBJECT    = (((Object_t) SUBJECT) * ((Object_t) SUBJECT)) / (((Object_t) PREDICATE) * ((Object_t) PREDICATE));

    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // Add triple for putting
    EXPECT_EQ(hxhim::PutDouble(&hx,
                               (void *)   &SUBJECT, sizeof(SUBJECT),
                               (void *)   &PREDICATE, sizeof(PREDICATE),
                               (double *) &OBJECT),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *put_results = hxhim::FlushPuts(&hx);
    ASSERT_NE(put_results, nullptr);

    // Make sure put succeeded
    EXPECT_EQ(put_results->Size(), (std::size_t) 1);
    HXHIM_CXX_RESULTS_LOOP(put_results) {
        hxhim_op_t op;
        EXPECT_EQ(put_results->Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_PUT);

        int status = HXHIM_ERROR;
        EXPECT_EQ(put_results->Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);
    }

    hxhim::Results::Destroy(put_results);

    // Add subject-predicate to get back
    EXPECT_EQ(hxhim::GetDouble(&hx,
                               (void *)&SUBJECT, sizeof(SUBJECT),
                               (void *)&PREDICATE, sizeof(PREDICATE)),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *get_results = hxhim::FlushGets(&hx);
    ASSERT_NE(get_results, nullptr);

    // get the results and compare them with the original data
    EXPECT_EQ(get_results->Size(), (std::size_t) 1);
    HXHIM_CXX_RESULTS_LOOP(get_results) {
        hxhim_op_t op = hxhim_op_t::HXHIM_INVALID;
        EXPECT_EQ(get_results->Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_GET);

        int status = HXHIM_ERROR;
        EXPECT_EQ(get_results->Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);

        Subject_t *subject = nullptr;
        EXPECT_EQ(get_results->Subject((void **) &subject, nullptr), HXHIM_SUCCESS);
        EXPECT_EQ(*subject, SUBJECT);

        Predicate_t *predicate = nullptr;
        EXPECT_EQ(get_results->Predicate((void **) &predicate, nullptr), HXHIM_SUCCESS);
        EXPECT_EQ(*predicate, PREDICATE);

        hxhim_object_type_t object_type = hxhim_object_type_t::HXHIM_OBJECT_TYPE_INVALID;
        EXPECT_EQ(get_results->ObjectType(&object_type), HXHIM_SUCCESS);

        void *ptr = nullptr;
        std::size_t len = 0;
        get_results->Object(&ptr, &len);

        if (object_type == hxhim_object_type_t::HXHIM_OBJECT_TYPE_FLOAT) {
            EXPECT_FLOAT_EQ(* (float *) ptr, OBJECT);
        }
        else if (object_type == hxhim_object_type_t::HXHIM_OBJECT_TYPE_DOUBLE) {
            EXPECT_DOUBLE_EQ(* (double *) ptr, OBJECT);
        }
        else {
            FAIL();
        }
    }

    hxhim::Results::Destroy(get_results);

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
