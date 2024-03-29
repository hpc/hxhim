#include <cstring>
#include <sstream>

#include <gtest/gtest.h>
#include <leveldb/db.h>
#include <mpi.h>

#include "datastore/LevelDB.hpp"
#include "datastore/triplestore.hpp"
#include "rm_r.hpp"
#include "triples.hpp"
#include "utils/memory.hpp"

class LevelDBTest : public ::Datastore::LevelDB {
    public:
        LevelDBTest(const int rank)
            : ::Datastore::LevelDB(rank, 0, nullptr, true)
        {}

        ~LevelDBTest()  {
            Close();
            cleanup();
        }

        int GetHistograms(Datastore::Datastore::Histograms **histograms) {
            if (histograms) {
                *histograms = &hists;
            }

            return DATASTORE_SUCCESS;
        }

        ::leveldb::DB *data() const {
            return db;
        }

    private:
        void cleanup() {
            rm_r(Name().c_str());
        }
};

// create a test LevelDB datastore and insert some triples
static LevelDBTest *setup() {
    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    std::stringstream s;
    s << "LEVELDB-TEST-" << rank;

    rm_r(s.str());

    LevelDBTest *ds = construct<LevelDBTest>(rank);
    EXPECT_FALSE(ds->Usable());
    ds->Open(s.str());
    EXPECT_TRUE(ds->Usable());

    Message::Request::BPut req(count);

    for(std::size_t i = 0; i < count; i++) {
        req.add(Blob(subjects[i]),
                Blob(predicates[i]),
                Blob(objects[i]));
    }

    destruct(ds->operate(&req));

    return ds;
}

TEST(LevelDB, BPut) {
    LevelDBTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    ::leveldb::DB *db = ds->data();

    // read directly from leveldb since setup() already did PUTs
    for(std::size_t i = 0; i < count; i++) {
        Blob key;
        EXPECT_EQ(sp_to_key(Blob(subjects[i]), Blob(predicates[i]), &key), HXHIM_SUCCESS);
        EXPECT_NE(key.size(), 0);

        std::string value;
        ::leveldb::Status status = db->Get(::leveldb::ReadOptions(),
                                           ::leveldb::Slice((char *) key.data(), key.size()),
                                           &value);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(memcmp(objects[i].data(), value.c_str(), value.size() - sizeof(hxhim_data_t)), 0);
    }

    // make sure datastore only has count items
    std::size_t items = 0;
    ::leveldb::Iterator *it = db->NewIterator(::leveldb::ReadOptions());
    for(it->SeekToFirst(); it->Valid(); it->Next()) {
        items++;
    }
    EXPECT_EQ(it->status().ok(), true);
    EXPECT_EQ(items, count);
    delete it;

    destruct(ds);
}

TEST(LevelDB, BGet) {
    LevelDBTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    // get triple back using GET
    // include the non-existant subject-predicate pair
    Message::Request::BGet req(count + 1);
    for(std::size_t i = 0; i < count + 1; i++) {
        req.add(Blob(subjects[i]),
                Blob(predicates[i]),
                Blob(objects[i]).data_type());
    }

    Message::Response::BGet *res = ds->operate(&req);
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->count, count + 1);

    // first count results should be good
    for(std::size_t i = 0; i < count; i++) {
        EXPECT_EQ(res->statuses[i], DATASTORE_SUCCESS);
        ASSERT_NE(res->objects[i].data(), nullptr);
        EXPECT_EQ(res->objects[i].size(), objects[i].size());
        EXPECT_EQ(std::memcmp(objects[i].data(), res->objects[i].data(), res->objects[i].size()), 0);
    }

    // final result should be bad
    EXPECT_EQ(res->statuses[count], DATASTORE_ERROR);
    EXPECT_EQ(res->objects[count].data(), nullptr);

    destruct(res);
    destruct(ds);
}

TEST(LevelDB, BGetOp) {
    LevelDBTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    for(int op = HXHIM_GETOP_EQ; op < HXHIM_GETOP_INVALID; op++) {
        Message::Request::BGetOp req(1);
        req.add(Blob(subjects[0]),
                Blob(predicates[0]),
                hxhim_data_t::HXHIM_DATA_BYTE,
                1,
                static_cast<hxhim_getop_t>(op));

        Message::Response::BGetOp *res = ds->operate(&req);
        ASSERT_NE(res, nullptr);
        EXPECT_EQ(res->count, req.count);

        switch (req.ops[0]) {
            case HXHIM_GETOP_EQ:
                EXPECT_EQ(res->num_recs[0], 1);

                // all results are the same value
                for(std::size_t j = 0; j < res->num_recs[0]; j++) {
                    ASSERT_NE(res->subjects[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(subjects[0].data(),
                                     res->subjects[0][j].data(),
                                     res->subjects[0][j].size()),   0);

                    ASSERT_NE(res->predicates[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(predicates[0].data(),
                                     res->predicates[0][j].data(),
                                     res->predicates[0][j].size()), 0);

                    ASSERT_NE(res->objects[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(objects[0].data(),
                                     res->objects[0][j].data(),
                                     res->objects[0][j].size()),    0);
                }
                break;
            case HXHIM_GETOP_NEXT:
            case HXHIM_GETOP_FIRST:
            case HXHIM_GETOP_LAST:
                EXPECT_EQ(res->num_recs[0], 1);
                // not sure how to test these
                break;
            case HXHIM_GETOP_PREV:
                EXPECT_EQ(res->num_recs[0], 1);

                for(std::size_t j = 0; j < res->num_recs[0]; j++) {
                    ASSERT_NE(res->subjects[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(subjects[j].data(),
                                     res->subjects[0][j].data(),
                                     res->subjects[0][j].size()),   0);

                    ASSERT_NE(res->predicates[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(predicates[j].data(),
                                     res->predicates[0][j].data(),
                                     res->predicates[0][j].size()), 0);

                    ASSERT_NE(res->objects[0][j].data(), nullptr);
                    EXPECT_EQ(memcmp(objects[j].data(),
                                     res->objects[0][j].data(),
                                     res->objects[0][j].size()),    0);
                }
                break;
            case HXHIM_GETOP_INVALID:
            default:
                break;
        }

        destruct(res);
    }

    destruct(ds);
}

TEST(LevelDB, BDelete) {
    LevelDBTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    ::leveldb::DB *db = ds->data();

    // delete the triples
    {
        // include the non-existant subject-predicate pair
        Message::Request::BDelete req(count + 1);
        for(std::size_t i = 0; i < count + 1; i++) {
            req.add(Blob(subjects[i]),
                    Blob(predicates[i]));
        }

        Message::Response::BDelete *res = ds->operate(&req);
        ASSERT_NE(res, nullptr);
        EXPECT_EQ(res->count, count + 1);
        for(std::size_t i = 0; i < count; i++) {
            EXPECT_EQ(res->statuses[i], DATASTORE_SUCCESS);
        }

        // as long as one delete succeeded, will return HXHIM_SUCCESS
        EXPECT_EQ(res->statuses[count], DATASTORE_SUCCESS);

        destruct(res);
    }

    // check if the triples still exist using GET
    {
        // include the non-existant subject-predicate pair
        Message::Request::BGet req(count + 1);
        for(std::size_t i = 0; i < count + 1; i++) {
            req.subjects[i]   = Blob(subjects[i]);
            req.predicates[i] = Blob(predicates[i]);
            req.count++;
        }

        Message::Response::BGet *res = ds->operate(&req);
        ASSERT_NE(res, nullptr);
        EXPECT_EQ(res->count, count + 1);
        for(std::size_t i = 0; i < res->count; i++) {
            EXPECT_EQ(res->statuses[i], DATASTORE_ERROR);
        }

        destruct(res);
    }

    // check the datastore directly
    {
        for(std::size_t i = 0; i < count; i++) {
            Blob key;
            EXPECT_EQ(sp_to_key(Blob(subjects[i]), Blob(predicates[i]), &key), HXHIM_SUCCESS);
            EXPECT_NE(key.size(), 0);

            std::string value;
            ::leveldb::Status status = db->Get(::leveldb::ReadOptions(),
                                               ::leveldb::Slice((char *) key.data(), key.size()),
                                               &value);
            EXPECT_EQ(status.ok(), false);
            EXPECT_EQ(memcmp(objects[i].data(), value.c_str(), value.size()), 0);
        }
    }

    // make sure datastore doesn't have the original SPO triples
    {
        std::size_t items = 0;
        ::leveldb::Iterator *it = db->NewIterator(::leveldb::ReadOptions());
        for(it->SeekToFirst(); it->Valid(); it->Next()) {
            items++;
        }
        EXPECT_EQ(it->status().ok(), true);
        EXPECT_EQ(items, 0);
        delete it;
    }

    destruct(ds);
}

TEST(LevelDB, Histograms) {
    const std::string hist_names[] = {"hist0", "hist1"};
    const std::size_t hist_count = sizeof(hist_names) / sizeof(*hist_names);
    const std::size_t first_ns[] = {count - 1,   //  buckets are generated at the end
                                    count + 1};  //  buckets are not generated at the end
    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    std::stringstream s;
    s << "LEVELDB-TEST-" << rank;

    rm_r(s.str());

    LevelDBTest *ds = construct<LevelDBTest>(rank);
    ds->Open(s.str());

    // add some histograms
    for(std::size_t i = 0; i < sizeof(hist_names) / sizeof(*hist_names); i++) {
        Histogram::Histogram *hist = construct<Histogram::Histogram>(
                                         Histogram::Config{
                                             first_ns[i],
                                             [](const double *, const size_t,
                                                double **buckets, size_t *size,
                                                void *) -> int {
                                                 if (!buckets || !size) {
                                                     return HISTOGRAM_ERROR;
                                                 }

                                                 *size = 1;
                                                 *buckets = alloc_array<double>(*size + 1);
                                                 (*buckets)[0] = 0;

                                                 return HISTOGRAM_SUCCESS;
                                             },
                                             nullptr
                                         },
                                         hist_names[i]);
        ds->AddHistogram(hist_names[i], hist);
    }

    // insert values so that the histograms get some data
    const std::size_t triples = count * 2;
    double *values = new double[triples];
    Message::Request::BPut req(triples);
    for(std::size_t i = 0; i < triples; i += 2) {
        values[i] = (double) i;
        Blob ref(&values[i], sizeof(values[i]), hxhim_data_t::HXHIM_DATA_DOUBLE);

        req.add(ref,
                ReferenceBlob((void *) hist_names[0].data(),
                              hist_names[0].size(),
                              hxhim_data_t::HXHIM_DATA_BYTE),
                ref);

        req.add(ref,
                ReferenceBlob((void *) hist_names[1].data(),
                              hist_names[1].size(),
                              hxhim_data_t::HXHIM_DATA_BYTE),
                ref);
    }

    destruct(ds->operate(&req));
    delete [] values;

    Datastore::Datastore::Histograms *hists = nullptr;
    EXPECT_EQ(ds->GetHistograms(&hists), DATASTORE_SUCCESS);
    ASSERT_NE(hists, nullptr);
    EXPECT_EQ(hists->size(), hist_count);

    // Check that the histograms have data
    {
        double *buckets = nullptr;
        std::size_t *counts = nullptr;
        std::size_t size = 0;

        // hist0 buckets have been generated
        {
            Datastore::Datastore::Histograms::const_iterator it = hists->find(hist_names[0]);
            if (it == hists->end()) {
                FAIL();
            }
            EXPECT_EQ(it->second->get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);

            EXPECT_EQ(size, 1);

            ASSERT_NE(buckets, nullptr);
            EXPECT_EQ(buckets[0], 0);

            ASSERT_NE(counts, nullptr);
            EXPECT_EQ(counts[0], count);
        }

        // hist1 buckets have not been generated
        {
            Datastore::Datastore::Histograms::const_iterator it = hists->find(hist_names[1]);
            if (it == hists->end()) {
                FAIL();
            }

            EXPECT_EQ(it->second->get(&buckets, &counts, &size), HISTOGRAM_ERROR);

            std::size_t first_n = 0;
            double *cache = nullptr;
            std::size_t size = 0;
            EXPECT_EQ(it->second->get_cache(&first_n, &cache, &size), HISTOGRAM_SUCCESS);

            EXPECT_EQ(first_n, first_ns[1]);
            EXPECT_NE(cache, nullptr);
            EXPECT_EQ(size, count);
        }
    }

    // Write the histograms to the datastore
    ds->WriteHistograms();
    {
        ::leveldb::Iterator* it = ds->data()->NewIterator(::leveldb::ReadOptions());
        std::size_t pairs = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            pairs++;
        }
        ASSERT_TRUE(it->status().ok());
        delete it;

        EXPECT_EQ(pairs, triples + hist_count);
    }

    // clear the histogram data
    for(Datastore::Datastore::Histograms::value_type &hist : *hists) {
        hist.second->clear();

        double *buckets = nullptr;
        std::size_t *counts = nullptr;
        std::size_t size = 0;
        EXPECT_EQ(hist.second->get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);

        EXPECT_EQ(buckets, nullptr);
        EXPECT_EQ(counts, nullptr);
        EXPECT_EQ(size, 0);

        std::size_t first_n = 0;
        double *cache = nullptr;
        std::size_t cache_size = 0;
        EXPECT_EQ(hist.second->get_cache(&first_n, &cache, &cache_size), HISTOGRAM_SUCCESS);

        EXPECT_EQ(first_n, 0);
        EXPECT_NE(cache, nullptr);
        EXPECT_EQ(size, 0);
    }

    EXPECT_EQ(hists->size(), hist_count);

    // read the histograms back in
    EXPECT_EQ(ds->ReadHistograms(Datastore::HistNames_t(std::begin(hist_names),
                                                        std::end  (hist_names))),
              hist_count);

    // Check that the recovered histograms have data
    {
        double *buckets = nullptr;
        std::size_t *counts = nullptr;
        std::size_t size = 0;

        // hist0 has buckets
        {
            Datastore::Datastore::Histograms::const_iterator it = hists->find(hist_names[0]);
            if (it == hists->end()) {
                FAIL();
            }
            EXPECT_EQ(it->second->get(&buckets, &counts, &size), HISTOGRAM_SUCCESS);

            EXPECT_EQ(size, 1);

            ASSERT_NE(buckets, nullptr);
            EXPECT_EQ(buckets[0], 0);

            ASSERT_NE(counts, nullptr);
            EXPECT_EQ(counts[0], count);
        }

        // hist1 does not have buckets
        {
            Datastore::Datastore::Histograms::const_iterator it = hists->find(hist_names[1]);
            if (it == hists->end()) {
                FAIL();
            }

            EXPECT_EQ(it->second->get(&buckets, &counts, &size), HISTOGRAM_ERROR);

            std::size_t first_n = 0;
            double *cache = nullptr;
            std::size_t size = 0;
            EXPECT_EQ(it->second->get_cache(&first_n, &cache, &size), HISTOGRAM_SUCCESS);

            EXPECT_EQ(first_n, first_ns[1]);
            EXPECT_NE(cache, nullptr);
            EXPECT_EQ(size, count);
        }
    }

    destruct(ds);
}
