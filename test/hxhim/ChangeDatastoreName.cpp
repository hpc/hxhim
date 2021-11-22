#include <gtest/gtest.h>

#include "datastore/constants.hpp"
#include "datastore/rm_r.hpp"
#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"

typedef uint64_t Subject_t;
typedef uint64_t Predicate_t;
typedef double   Object_t;

static const char PREFIX[]   = "/tmp/hxhim-text";
static const char OLD_NAME[] = "old_name";
static const char NEW_NAME[] = "new_name";
static const char POSTFIX[]  = "";

static void ChangeDatastoreName(::Datastore::Type type) {
    const Subject_t   SUBJECT   = (((Subject_t)   rand()) << 32) | rand();
    const Predicate_t PREDICATE = (((Predicate_t) rand()) << 32) | rand();
    const Object_t    OBJECT    = (((Object_t) SUBJECT) * ((Object_t) SUBJECT)) / (((Object_t) PREDICATE) * ((Object_t) PREDICATE));

    // clean up existing datastores
    rm_r(PREFIX);

    hxhim_t hx;
    ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(fill_options(&hx), true);
    ASSERT_EQ(hxhim_set_datastore_name(&hx, PREFIX, OLD_NAME, POSTFIX), HXHIM_SUCCESS);

    switch(type) {
        case ::Datastore::IN_MEMORY:
            hxhim_set_datastore_in_memory(&hx);
            break;
        #if HXHIM_HAVE_LEVELDB
        case ::Datastore::LEVELDB:
            hxhim_set_datastore_leveldb(&hx, true);
            break;
        #endif
        #if HXHIM_HAVE_ROCKSDB
        case ::Datastore::ROCKSDB:
            hxhim_set_datastore_rocksdb(&hx, true);
            break;
        #endif
        default:
            hxhim::Close(&hx);
            FAIL();
            break;
    }

    ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);

    // Add triple for putting
    EXPECT_EQ(hxhim::PutDouble(&hx,
                               (void *)   &SUBJECT,   sizeof(SUBJECT),   hxhim_data_t::HXHIM_DATA_UINT64,
                               (void *)   &PREDICATE, sizeof(PREDICATE), hxhim_data_t::HXHIM_DATA_UINT64,
                               (double *) &OBJECT,
                               HXHIM_PUT_SPO),
              HXHIM_SUCCESS);

    // Flush all queued items
    hxhim::Results *put_results = hxhim::FlushPuts(&hx);
    ASSERT_NE(put_results, nullptr);

    // Make sure put succeeded
    EXPECT_EQ(put_results->Size(), 1);
    HXHIM_CXX_RESULTS_LOOP(put_results) {
        hxhim_op_t op;
        EXPECT_EQ(put_results->Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_PUT);

        int status = HXHIM_ERROR;
        EXPECT_EQ(put_results->Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);
    }

    hxhim::Results::Destroy(put_results);

    // GET from original datastores
    {
        // Get back the object for the original subject predicate
        EXPECT_EQ(hxhim::GetDouble(&hx,
                                   (void *)&SUBJECT,   sizeof(SUBJECT),   hxhim_data_t::HXHIM_DATA_UINT64,
                                   (void *)&PREDICATE, sizeof(PREDICATE), hxhim_data_t::HXHIM_DATA_UINT64),
                  HXHIM_SUCCESS);

        // Flush all queued items
        hxhim::Results *get_results = hxhim::FlushGets(&hx);
        ASSERT_NE(get_results, nullptr);

        // GET the results and compare them with the original data
        EXPECT_EQ(get_results->Size(), (std::size_t) 1);
        HXHIM_CXX_RESULTS_LOOP(get_results) {
            hxhim_op_t op = hxhim_op_t::HXHIM_INVALID;
            EXPECT_EQ(get_results->Op(&op), HXHIM_SUCCESS);
            EXPECT_EQ(op, hxhim_op_t::HXHIM_GET);

            int status = HXHIM_ERROR;
            EXPECT_EQ(get_results->Status(&status), HXHIM_SUCCESS);
            EXPECT_EQ(status, HXHIM_SUCCESS);

            Subject_t *subject = nullptr;
            std::size_t subject_len = 0;
            hxhim_data_t subject_type;
            EXPECT_EQ(get_results->Subject((void **) &subject, &subject_len, &subject_type), HXHIM_SUCCESS);
            EXPECT_EQ(*subject, SUBJECT);
            EXPECT_EQ(subject_len, sizeof(SUBJECT));
            EXPECT_EQ(subject_type, hxhim_data_t::HXHIM_DATA_UINT64);

            Predicate_t *predicate = nullptr;
            std::size_t predicate_len = 0;
            hxhim_data_t predicate_type;
            EXPECT_EQ(get_results->Predicate((void **) &predicate, &predicate_len, &predicate_type), HXHIM_SUCCESS);
            EXPECT_EQ(*predicate, PREDICATE);
            EXPECT_EQ(predicate_len, sizeof(PREDICATE));
            EXPECT_EQ(predicate_type, hxhim_data_t::HXHIM_DATA_UINT64);

            Object_t *object = nullptr;
            std::size_t object_size = 0;
            hxhim_data_t object_type;
            get_results->Object((void **) &object, &object_size, &object_type);

            if (object_type == hxhim_data_t::HXHIM_DATA_FLOAT) {
                EXPECT_NEAR(* (float *) object, OBJECT, std::numeric_limits<float>::digits10);
                EXPECT_EQ(object_size, sizeof(float));
            }
            else if (object_type == hxhim_data_t::HXHIM_DATA_DOUBLE) {
                EXPECT_NEAR(* (double *) object, OBJECT, std::numeric_limits<double>::digits10);
                EXPECT_EQ(object_size, sizeof(double));
            }
            else {
                FAIL();
            }
        }

        hxhim::Results::Destroy(get_results);
    }

    // change datastores
    hxhim::Results *change_results = hxhim::ChangeDatastoreName(&hx, NEW_NAME,
                                                                false, false, true);

    // get a SYNC response
    ASSERT_NE(change_results, nullptr);
    EXPECT_EQ(change_results->Size(), (std::size_t) 1);
    HXHIM_CXX_RESULTS_LOOP(change_results) {
        hxhim_op_t op = hxhim_op_t::HXHIM_INVALID;
        EXPECT_EQ(change_results->Op(&op), HXHIM_SUCCESS);
        EXPECT_EQ(op, hxhim_op_t::HXHIM_SYNC);

        int status = HXHIM_ERROR;
        EXPECT_EQ(change_results->Status(&status), HXHIM_SUCCESS);
        EXPECT_EQ(status, HXHIM_SUCCESS);
    }
    hxhim::Results::Destroy(change_results);

    // GET from new datastores
    {
        EXPECT_EQ(hxhim::GetDouble(&hx,
                                   (void *)&SUBJECT,   sizeof(SUBJECT),   hxhim_data_t::HXHIM_DATA_UINT64,
                                   (void *)&PREDICATE, sizeof(PREDICATE), hxhim_data_t::HXHIM_DATA_UINT64),
                  HXHIM_SUCCESS);

        hxhim::Results *get_results = hxhim::FlushGets(&hx);
        ASSERT_NE(get_results, nullptr);

        EXPECT_EQ(get_results->Size(), (std::size_t) 1);

        // GET failed since the datastore does not exist
        HXHIM_CXX_RESULTS_LOOP(get_results) {
            hxhim_op_t op = hxhim_op_t::HXHIM_INVALID;
            EXPECT_EQ(get_results->Op(&op), HXHIM_SUCCESS);
            EXPECT_EQ(op, hxhim_op_t::HXHIM_GET);

            int status = HXHIM_ERROR;
            EXPECT_EQ(get_results->Status(&status), HXHIM_SUCCESS);
            EXPECT_EQ(status, HXHIM_ERROR);
        }

        hxhim::Results::Destroy(get_results);
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);

    // clean up artifacts
    rm_r(PREFIX);
}

TEST(InMemory, ChangeDatastoreName) {
    ChangeDatastoreName(::Datastore::IN_MEMORY);
}

#if HXHIM_HAVE_LEVELDB
TEST(LevelDB, ChangeDatastoreName) {
    ChangeDatastoreName(::Datastore::LEVELDB);
}
#endif

#if HXHIM_HAVE_ROCKSDB
TEST(RocksDB, ChangeDatastoreName) {
    ChangeDatastoreName(::Datastore::ROCKSDB);
}
#endif
