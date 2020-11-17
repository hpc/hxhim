#include <vector>
#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

typedef uint64_t Subject_t;
typedef double   Predicate_t;
typedef int32_t  Object_t;

struct Triple {
    Subject_t subject;
    Predicate_t predicate;
    Object_t object;
};

TEST(hxhim, PutGetOp) {
    const std::size_t COUNT = 10;
    std::vector<Triple> triples(COUNT);

    // simple ordered data
    for(std::size_t i = 0; i < COUNT; i++) {
        triples[i].subject   = 0;
        triples[i].predicate = i;
        triples[i].object    = - ((Object_t) i);
    }

    hxhim_options_t opts;
    ASSERT_EQ(fill_options(&opts), true);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Open(&hx, &opts), HXHIM_SUCCESS);

    // Add triples for putting
    for(struct Triple &triple : triples) {
        EXPECT_EQ(hxhim::Put(&hx,
                             (void *) &triple.subject,   sizeof(triple.subject),   hxhim_data_t::HXHIM_DATA_UINT64,
                             (void *) &triple.predicate, sizeof(triple.predicate), hxhim_data_t::HXHIM_DATA_DOUBLE,
                             (void *) &triple.object,    sizeof(triple.object),    hxhim_data_t::HXHIM_DATA_INT32),
                  HXHIM_SUCCESS);
    }

    // Flush all queued items
    hxhim::Results *put_results = hxhim::Flush(&hx);
    ASSERT_NE(put_results, nullptr);

    // Make sure put succeeded
    EXPECT_EQ(put_results->Size(), COUNT * (std::size_t) HXHIM_PUT_MULTIPLIER);
    HXHIM_CXX_RESULTS_LOOP(put_results) {
        hxhim_op_t op;
        EXPECT_EQ(put_results->Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_PUT);

        int status = HXHIM_ERROR;
        EXPECT_EQ(put_results->Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);
    }

    hxhim::Results::Destroy(put_results);

    // incrementally get as many triples as there are available
    for(std::size_t i = 0; i < COUNT; i++) {
        // Add subject-predicate to get back
        EXPECT_EQ(hxhim::GetOp(&hx,
                               (void *) &triples[i].subject,   sizeof(triples[i].subject),   hxhim_data_t::HXHIM_DATA_UINT64,
                               (void *) &triples[i].predicate, sizeof(triples[i].predicate), hxhim_data_t::HXHIM_DATA_DOUBLE,
                               hxhim_data_t::HXHIM_DATA_INT32,
                               COUNT, hxhim_getop_t::HXHIM_GETOP_NEXT),
                  HXHIM_SUCCESS);

        // Flush all queued items
        hxhim::Results *getop_results = hxhim::Flush(&hx);
        ASSERT_NE(getop_results, nullptr);

        std::size_t j = i;

        // get the results and compare them with the original data
        EXPECT_EQ(getop_results->Size(), COUNT - i);
        HXHIM_CXX_RESULTS_LOOP(getop_results) {
            hxhim_op_t op = hxhim_op_t::HXHIM_INVALID;
            EXPECT_EQ(getop_results->Op(&op), HXHIM_SUCCESS);
            EXPECT_EQ(op, hxhim_op_t::HXHIM_GETOP);

            int status = HXHIM_ERROR;
            EXPECT_EQ(getop_results->Status(&status), HXHIM_SUCCESS);
            EXPECT_EQ(status, HXHIM_SUCCESS);

            Subject_t *subject = nullptr;
            std::size_t subject_len = 0;
            hxhim_data_t subject_type = hxhim_data_t::HXHIM_DATA_INVALID;
            EXPECT_EQ(getop_results->Subject((void **) &subject, &subject_len, &subject_type), HXHIM_SUCCESS);
            EXPECT_EQ(*subject, triples[j].subject);
            EXPECT_EQ(subject_len, sizeof(triples[j].subject));
            EXPECT_EQ(subject_type, hxhim_data_t::HXHIM_DATA_UINT64);

            Predicate_t *predicate = nullptr;
            std::size_t predicate_len = 0;
            hxhim_data_t predicate_type = hxhim_data_t::HXHIM_DATA_INVALID;
            EXPECT_EQ(getop_results->Predicate((void **) &predicate, &predicate_len, &predicate_type), HXHIM_SUCCESS);
            EXPECT_NEAR(*predicate, triples[j].predicate, std::numeric_limits<Predicate_t>::digits10);
            EXPECT_EQ(predicate_len, sizeof(triples[j].predicate));
            EXPECT_EQ(predicate_type, hxhim_data_t::HXHIM_DATA_DOUBLE);

            Object_t *object = nullptr;
            std::size_t object_len = 0;
            hxhim_data_t object_type = hxhim_data_t::HXHIM_DATA_INVALID;
            getop_results->Object((void **) &object, &object_len, &object_type);

            EXPECT_EQ(*object, triples[j].object);
            EXPECT_EQ(object_len, sizeof(triples[j].object));
            EXPECT_EQ(object_type, hxhim_data_t::HXHIM_DATA_INT32);

            j++;
        }

        hxhim::Results::Destroy(getop_results);
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
    EXPECT_EQ(hxhim_options_destroy(&opts), HXHIM_SUCCESS);
}
