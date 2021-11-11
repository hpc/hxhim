#include <gtest/gtest.h>

#include "generic_options.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"

/*
 * Test the insertion of operations into HXHIM's queues
 */

const std::string TEST_HIST_NAME = "Test Histogram Name";

const char *SUBJECTS[]   = {"SUBJECT0",   "SUBJECT1"};
const char *PREDICATES[] = {"PREDICATE0", "PREDICATE1"};
const char *OBJECTS[]    = {"OBJECT0",    "OBJECT1"};

const hxhim_data_t TYPE = hxhim_data_t::HXHIM_DATA_BYTE;

void enqueue_put(bool async_puts) {
    hxhim_t hx;
    ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(fill_options(&hx), true);
    ASSERT_EQ(hxhim_set_maximum_ops_per_request(&hx, 1), HXHIM_SUCCESS);
    ASSERT_EQ(hxhim_set_maximum_size_per_request(&hx, 0), HXHIM_SUCCESS);
    if (async_puts) {
        ASSERT_EQ(hxhim_set_start_async_puts_at(&hx, 3), HXHIM_SUCCESS); // don't clear PUT queues
    }

    ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);

    const int rank = hx.p->bootstrap.rank;
    const int size = hx.p->bootstrap.size;

    hxhim::Queues<Message::Request::BPut> &puts = hx.p->queues.puts.queue;
    EXPECT_EQ(puts.size(), (std::size_t) size);

    // enqueue one PUT
    EXPECT_EQ(hxhim::PutImpl(&hx,
                             puts,
                             ReferenceBlob((char *) SUBJECTS[0],   strlen(SUBJECTS[0]),   hxhim_data_t::HXHIM_DATA_BYTE),
                             ReferenceBlob((char *) PREDICATES[0], strlen(PREDICATES[0]), hxhim_data_t::HXHIM_DATA_BYTE),
                             ReferenceBlob((char *) OBJECTS[0],    strlen(OBJECTS[0]),    TYPE),
                             HXHIM_PUT_SPO),
              HXHIM_SUCCESS);
    ASSERT_EQ(puts[rank].size(), 1);

    {
        Message::Request::BPut *head = puts[rank].front();
        ASSERT_EQ(head->count, 1);
        EXPECT_EQ(head->subjects[0].data(), SUBJECTS[0]);
        EXPECT_EQ(head->subjects[0].size(), strlen(SUBJECTS[0]));
        EXPECT_EQ(head->predicates[0].data(), PREDICATES[0]);
        EXPECT_EQ(head->predicates[0].size(), strlen(PREDICATES[0]));
        EXPECT_EQ(head->objects[0].data(), OBJECTS[0]);
        EXPECT_EQ(head->objects[0].size(), strlen(OBJECTS[0]));
    }

    // enqueue a second PUT
    EXPECT_EQ(hxhim::PutImpl(&hx,
                             puts,
                             ReferenceBlob((char *) SUBJECTS[1],   strlen(SUBJECTS[1]),   hxhim_data_t::HXHIM_DATA_BYTE),
                             ReferenceBlob((char *) PREDICATES[1], strlen(PREDICATES[1]), hxhim_data_t::HXHIM_DATA_BYTE),
                             ReferenceBlob((char *) OBJECTS[1],    strlen(OBJECTS[1]),    TYPE),
                             HXHIM_PUT_SPO),
              HXHIM_SUCCESS);
    ASSERT_EQ(puts[rank].size(), 2); // maximum_ops_per_send is set to 1, so each PUT fills up a packet

    {
        Message::Request::BPut *head = puts[rank].front();
        ASSERT_EQ(head->count, 1);
        EXPECT_EQ(head->subjects[0].data(), SUBJECTS[0]);
        EXPECT_EQ(head->subjects[0].size(), strlen(SUBJECTS[0]));
        EXPECT_EQ(head->predicates[0].data(), PREDICATES[0]);
        EXPECT_EQ(head->predicates[0].size(), strlen(PREDICATES[0]));
        EXPECT_EQ(head->objects[0].data(), OBJECTS[0]);
        EXPECT_EQ(head->objects[0].size(), strlen(OBJECTS[0]));
    }

    {
        Message::Request::BPut *tail = puts[rank].back();
        ASSERT_EQ(tail->count, 1);
        EXPECT_EQ(tail->subjects[0].data(), SUBJECTS[1]);
        EXPECT_EQ(tail->subjects[0].size(), strlen(SUBJECTS[1]));
        EXPECT_EQ(tail->predicates[0].data(), PREDICATES[1]);
        EXPECT_EQ(tail->predicates[0].size(), strlen(PREDICATES[1]));
        EXPECT_EQ(tail->objects[0].data(), OBJECTS[1]);
        EXPECT_EQ(tail->objects[0].size(), strlen(OBJECTS[1]));
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
}

TEST(Enqueue, PUT) {
    enqueue_put(false);
    enqueue_put(true);
}

TEST(Enqueue, GET) {
    hxhim_t hx;
    ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(fill_options(&hx), true);

    ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);

    int rank = -1;
    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, &rank, &size), HXHIM_SUCCESS);
    EXPECT_GT(rank, -1);
    EXPECT_GT(size, -1);

    hxhim::Queues<Message::Request::BGet> &gets = hx.p->queues.gets;
    EXPECT_EQ(gets.size(), (std::size_t) size);

    // enqueue one GET
    EXPECT_EQ(hxhim::GetImpl(&hx,
                             gets,
                             ReferenceBlob((char *) SUBJECTS[0],   strlen(SUBJECTS[0]),   hxhim_data_t::HXHIM_DATA_BYTE),
                             ReferenceBlob((char *) PREDICATES[0], strlen(PREDICATES[0]), hxhim_data_t::HXHIM_DATA_BYTE),
                             TYPE),
              HXHIM_SUCCESS);
    ASSERT_EQ(gets[rank].size(), 1);

    {
        Message::Request::BGet *head = gets[rank].front();
        ASSERT_EQ(head->count, 1);
        EXPECT_EQ(head->subjects[0].data(), SUBJECTS[0]);
        EXPECT_EQ(head->subjects[0].size(), strlen(SUBJECTS[0]));
        EXPECT_EQ(head->predicates[0].data(), PREDICATES[0]);
        EXPECT_EQ(head->predicates[0].size(), strlen(PREDICATES[0]));
        EXPECT_EQ(head->object_types[0], TYPE);
    }

    // enqueue a second GET
    EXPECT_EQ(hxhim::GetImpl(&hx,
                             gets,
                             ReferenceBlob((char *) SUBJECTS[1],   strlen(SUBJECTS[1]),   hxhim_data_t::HXHIM_DATA_BYTE),
                             ReferenceBlob((char *) PREDICATES[1], strlen(PREDICATES[1]), hxhim_data_t::HXHIM_DATA_BYTE),
                             TYPE),
              HXHIM_SUCCESS);
    ASSERT_EQ(gets[rank].size(), 2); // maximum_ops_per_send is set to 1

    {
        Message::Request::BGet *head = gets[rank].front();
        ASSERT_EQ(head->count, 1);
        EXPECT_EQ(head->subjects[0].data(), SUBJECTS[0]);
        EXPECT_EQ(head->subjects[0].size(), strlen(SUBJECTS[0]));
        EXPECT_EQ(head->predicates[0].data(), PREDICATES[0]);
        EXPECT_EQ(head->predicates[0].size(), strlen(PREDICATES[0]));
        EXPECT_EQ(head->object_types[0], TYPE);
    }

    {
        Message::Request::BGet *tail = gets[rank].back();
        ASSERT_EQ(tail->count, 1);
        EXPECT_EQ(tail->subjects[0].data(), SUBJECTS[1]);
        EXPECT_EQ(tail->subjects[0].size(), strlen(SUBJECTS[1]));
        EXPECT_EQ(tail->predicates[0].data(), PREDICATES[1]);
        EXPECT_EQ(tail->predicates[0].size(), strlen(PREDICATES[1]));
        EXPECT_EQ(tail->object_types[0], TYPE);
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
}

TEST(Enqueue, GETOP) {
    hxhim_t hx;
    ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(fill_options(&hx), true);

    ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);

    int rank = -1;
    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, &rank, &size), HXHIM_SUCCESS);
    EXPECT_GT(rank, -1);
    EXPECT_GT(size, -1);

    hxhim::Queues<Message::Request::BGetOp> &getops = hx.p->queues.getops;
    EXPECT_EQ(getops.size(), (std::size_t) size);

    const std::size_t num_recs = rand();
    enum hxhim_getop_t op = HXHIM_GETOP_EQ;

    // enqueue one GETOP
    EXPECT_EQ(hxhim::GetOpImpl(&hx,
                             getops,
                             ReferenceBlob((char *) SUBJECTS[0],   strlen(SUBJECTS[0]),   hxhim_data_t::HXHIM_DATA_BYTE),
                             ReferenceBlob((char *) PREDICATES[0], strlen(PREDICATES[0]), hxhim_data_t::HXHIM_DATA_BYTE),
                             TYPE, num_recs, op),
              HXHIM_SUCCESS);
    ASSERT_EQ(getops[rank].size(), 1);

    {
        Message::Request::BGetOp *head = getops[rank].front();
        ASSERT_EQ(head->count, 1);
        EXPECT_EQ(head->subjects[0].data(), SUBJECTS[0]);
        EXPECT_EQ(head->subjects[0].size(), strlen(SUBJECTS[0]));
        EXPECT_EQ(head->predicates[0].data(), PREDICATES[0]);
        EXPECT_EQ(head->predicates[0].size(), strlen(PREDICATES[0]));
        EXPECT_EQ(head->object_types[0], TYPE);
        EXPECT_EQ(head->num_recs[0], num_recs);
        EXPECT_EQ(head->ops[0], op);
    }

    // enqueue a second GETOP
    EXPECT_EQ(hxhim::GetOpImpl(&hx,
                             getops,
                             ReferenceBlob((char *) SUBJECTS[1],   strlen(SUBJECTS[1]),   hxhim_data_t::HXHIM_DATA_BYTE),
                             ReferenceBlob((char *) PREDICATES[1], strlen(PREDICATES[1]), hxhim_data_t::HXHIM_DATA_BYTE),
                             TYPE, num_recs, op),
              HXHIM_SUCCESS);
    ASSERT_EQ(getops[rank].size(), 2); // maximum_ops_per_send is set to 1

    {
        Message::Request::BGetOp *head = getops[rank].front();
        ASSERT_EQ(head->count, 1);
        EXPECT_EQ(head->subjects[0].data(), SUBJECTS[0]);
        EXPECT_EQ(head->subjects[0].size(), strlen(SUBJECTS[0]));
        EXPECT_EQ(head->predicates[0].data(), PREDICATES[0]);
        EXPECT_EQ(head->predicates[0].size(), strlen(PREDICATES[0]));
        EXPECT_EQ(head->object_types[0], TYPE);
        EXPECT_EQ(head->num_recs[0], num_recs);
        EXPECT_EQ(head->ops[0], op);

        destruct(head);
    }

    {
        Message::Request::BGetOp *tail = getops[rank].back();
        ASSERT_EQ(tail->count, 1);
        EXPECT_EQ(tail->subjects[0].data(), SUBJECTS[1]);
        EXPECT_EQ(tail->subjects[0].size(), strlen(SUBJECTS[1]));
        EXPECT_EQ(tail->predicates[0].data(), PREDICATES[1]);
        EXPECT_EQ(tail->predicates[0].size(), strlen(PREDICATES[1]));
        EXPECT_EQ(tail->object_types[0], TYPE);
        EXPECT_EQ(tail->num_recs[0], num_recs);
        EXPECT_EQ(tail->ops[0], op);

        destruct(tail);
    }

    // prevent GETOPs from being sent during hxhim::Close
    getops[rank].clear();

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
}

TEST(Enqueue, DELETE) {
    hxhim_t hx;
    ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(fill_options(&hx), true);

    ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);

    int rank = -1;
    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, &rank, &size), HXHIM_SUCCESS);
    EXPECT_GT(rank, -1);
    EXPECT_GT(size, -1);

    hxhim::Queues<Message::Request::BDelete> &deletes = hx.p->queues.deletes;
    EXPECT_EQ(deletes.size(), (std::size_t) size);

    // enqueue one DELETE
    EXPECT_EQ(hxhim::DeleteImpl(&hx,
                             deletes,
                             ReferenceBlob((char *) SUBJECTS[0],   strlen(SUBJECTS[0]),   hxhim_data_t::HXHIM_DATA_BYTE),
                             ReferenceBlob((char *) PREDICATES[0], strlen(PREDICATES[0]), hxhim_data_t::HXHIM_DATA_BYTE)),
              HXHIM_SUCCESS);
    ASSERT_EQ(deletes[rank].size(), 1);

    {
        Message::Request::BDelete *head = deletes[rank].front();
        ASSERT_EQ(head->count, 1);
        EXPECT_EQ(head->subjects[0].data(), SUBJECTS[0]);
        EXPECT_EQ(head->subjects[0].size(), strlen(SUBJECTS[0]));
        EXPECT_EQ(head->predicates[0].data(), PREDICATES[0]);
        EXPECT_EQ(head->predicates[0].size(), strlen(PREDICATES[0]));
    }

    // enqueue a second DELETE
    EXPECT_EQ(hxhim::DeleteImpl(&hx,
                             deletes,
                             ReferenceBlob((char *) SUBJECTS[1],   strlen(SUBJECTS[1]),   hxhim_data_t::HXHIM_DATA_BYTE),
                             ReferenceBlob((char *) PREDICATES[1], strlen(PREDICATES[1]), hxhim_data_t::HXHIM_DATA_BYTE)),
              HXHIM_SUCCESS);
    ASSERT_EQ(deletes[rank].size(), 2); // maximum_ops_per_send is set to 1

    {
        Message::Request::BDelete *head = deletes[rank].front();
        ASSERT_EQ(head->count, 1);
        EXPECT_EQ(head->subjects[0].data(), SUBJECTS[0]);
        EXPECT_EQ(head->subjects[0].size(), strlen(SUBJECTS[0]));
        EXPECT_EQ(head->predicates[0].data(), PREDICATES[0]);
        EXPECT_EQ(head->predicates[0].size(), strlen(PREDICATES[0]));
    }

    {
        Message::Request::BDelete *tail = deletes[rank].back();
        ASSERT_EQ(tail->count, 1);
        EXPECT_EQ(tail->subjects[0].data(), SUBJECTS[1]);
        EXPECT_EQ(tail->subjects[0].size(), strlen(SUBJECTS[1]));
        EXPECT_EQ(tail->predicates[0].data(), PREDICATES[1]);
        EXPECT_EQ(tail->predicates[0].size(), strlen(PREDICATES[1]));
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
}

TEST(Enqueue, HISTOGRAM) {
    hxhim_t hx;
    ASSERT_EQ(hxhim::Init(&hx, MPI_COMM_WORLD), HXHIM_SUCCESS);
    ASSERT_EQ(fill_options(&hx), true);
    ASSERT_EQ(hxhim_add_histogram_track_predicate(&hx, TEST_HIST_NAME), HXHIM_SUCCESS);

    ASSERT_EQ(hxhim::Open(&hx), HXHIM_SUCCESS);

    int rank = -1;
    int size = -1;
    EXPECT_EQ(hxhim::GetMPI(&hx, nullptr, &rank, &size), HXHIM_SUCCESS);
    EXPECT_GT(rank, -1);
    EXPECT_GT(size, -1);

    hxhim::Queues<Message::Request::BHistogram> &histograms = hx.p->queues.histograms;
    EXPECT_EQ(histograms.size(), (std::size_t) size);

    // enqueue one HISTOGRAM
    EXPECT_EQ(hxhim::HistogramImpl(&hx,
                                   histograms,
                                   rank,
                                   TEST_HIST_NAME.data(), TEST_HIST_NAME.size()),
              HXHIM_SUCCESS);
    ASSERT_EQ(histograms[rank].size(), 1);

    {
        Message::Request::BHistogram *head = histograms[rank].front();
        ASSERT_EQ(head->count, 1);
    }

    // enqueue a second HISTOGRAM
    EXPECT_EQ(hxhim::HistogramImpl(&hx,
                                   histograms,
                                   rank,
                                   TEST_HIST_NAME.data(), TEST_HIST_NAME.size()),
              HXHIM_SUCCESS);
    ASSERT_EQ(histograms[rank].size(), 2); // maximum_ops_per_send is set to 1

    {
        Message::Request::BHistogram *head = histograms[rank].front();
        ASSERT_EQ(head->count, 1);
    }

    {
        Message::Request::BHistogram *tail = histograms[rank].back();
        ASSERT_EQ(tail->count, 1);
    }

    EXPECT_EQ(hxhim::Close(&hx), HXHIM_SUCCESS);
}
