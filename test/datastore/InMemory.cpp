#include <cstring>

#include <gtest/gtest.h>

#include "datastore/InMemory.hpp"
#include "hxhim/triplestore.hpp"
#include "triples.hpp"
#include "utils/memory.hpp"

class InMemoryTest : public datastore::InMemory {
  public:
    InMemoryTest()
        : datastore::InMemory(-1, 0, nullptr)
    {}

    int GetHistograms(datastore::Datastore::Histograms **histograms) {
        if (histograms) {
            *histograms = &hists;
        }

        return DATASTORE_SUCCESS;
    }

    std::map<std::string, std::string> const &data() const {
        return db;
    }
};

// create a test InMemory datastore and insert some triples
static InMemoryTest *setup() {
    InMemoryTest *ds = construct<InMemoryTest>();
    EXPECT_FALSE(ds->Usable());
    ds->Open();
    EXPECT_TRUE(ds->Usable());

    Transport::Request::BPut req(count);
    for(std::size_t i = 0; i < count; i++) {
        req.subjects[i]   = ReferenceBlob(subjects[i]);
        req.predicates[i] = ReferenceBlob(predicates[i]);
        req.objects[i]    = ReferenceBlob(objects[i]);
        req.count++;
    }

    destruct(ds->operate(&req));

    return ds;
}

TEST(InMemory, BPut) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    std::map<std::string, std::string> const &db = ds->data();
    EXPECT_EQ(db.size(), count);

    for(std::size_t i = 0; i < count; i++) {
        std::string key;
        EXPECT_EQ(sp_to_key(ReferenceBlob(subjects[i]), ReferenceBlob(predicates[i]), key), HXHIM_SUCCESS);
        EXPECT_EQ(key.size(),
                  ReferenceBlob(subjects[i]).pack_size(false) +
                  ReferenceBlob(predicates[i]).pack_size(false));
        EXPECT_EQ(db.find(key) != db.end(), true);
    }

    destruct(ds);
}

TEST(InMemory, BGet) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    // include the non-existant subject-predicate pair
    Transport::Request::BGet req(count + 1);
    for(std::size_t i = 0; i < count + 1; i++) {
        req.subjects[i]     = ReferenceBlob(subjects[i]);
        req.predicates[i]   = ReferenceBlob(predicates[i]);
        req.object_types[i] = hxhim_data_t::HXHIM_DATA_BYTE;
        req.count++;
    }

    Transport::Response::BGet *res = ds->operate(&req);
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->count, count + 1);
    for(std::size_t i = 0; i < count; i++) {
        EXPECT_EQ(res->statuses[i], DATASTORE_SUCCESS);
        ASSERT_NE(res->objects[i].data(), nullptr);
        EXPECT_EQ(res->objects[i].size(), ReferenceBlob(objects[i]).size());
        EXPECT_EQ(std::memcmp(ReferenceBlob(objects[i]).data(),
                              res->objects[i].data(),
                              res->objects[i].size()),
                  0);
    }

    EXPECT_EQ(res->statuses[count], DATASTORE_ERROR);
    EXPECT_EQ(res->objects[count].data(), nullptr);

    destruct(res);
    destruct(ds);
}

TEST(InMemory, BGetOp) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    for(int op = HXHIM_GETOP_EQ; op < HXHIM_GETOP_INVALID; op++) {
        Transport::Request::BGetOp req(1);

        req.subjects[0]     = ReferenceBlob(subjects[0]);
        req.predicates[0]   = ReferenceBlob(predicates[0]);
        req.object_types[0] = hxhim_data_t::HXHIM_DATA_BYTE;
        req.num_recs[0]     = 1;
        req.ops[0]          = static_cast<hxhim_getop_t>(op);
        req.count++;

        Transport::Response::BGetOp *res = ds->operate(&req);
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

TEST(InMemory, BDelete) {
    InMemoryTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    std::map<std::string, std::string> const &db = ds->data();
    EXPECT_EQ(db.size(), count);

    // include the non-existant subject-predicate pair
    Transport::Request::BDelete req(count + 1);
    for(std::size_t i = 0; i < count + 1; i++) {
        req.subjects[i]   = ReferenceBlob(subjects[i]);
        req.predicates[i] = ReferenceBlob(predicates[i]);
        req.count++;
    }

    Transport::Response::BDelete *res = ds->operate(&req);
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->count, count + 1);
    for(std::size_t i = 0; i < count; i++) {
        EXPECT_EQ(res->statuses[i], DATASTORE_SUCCESS);
    }

    EXPECT_EQ(res->statuses[count], DATASTORE_ERROR);

    destruct(res);

    EXPECT_EQ(db.size(), 0);

    // get directly from internal data
    for(std::size_t i = 0; i < count; i++) {
        const Blob sub = ReferenceBlob(subjects[i]);
        const Blob pred = ReferenceBlob(predicates[i]);
        std::string key;
        EXPECT_EQ(sp_to_key(sub, pred, key), HXHIM_SUCCESS);
        EXPECT_EQ(key.size(), sub.pack_size(false) + pred.pack_size(false));
        EXPECT_EQ(db.find(key) == db.end(), true);
    }

    destruct(ds);
}

TEST(InMemory, Histograms) {
    const std::string hist_names[] = {"hist0", "hist1"};
    const std::size_t hist_count = sizeof(hist_names) / sizeof(*hist_names);
    const std::size_t first_ns[] = {count - 1,   //  buckets are generated at the end
                                    count + 1};  //  buckets are not generated at the end

    InMemoryTest *ds = construct<InMemoryTest>();
    ds->Open();

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
    Transport::Request::BPut req(triples);
    for(std::size_t i = 0; i < triples; i += 2) {
        values[i] = (double) i;
        Blob ref(&values[i], sizeof(values[i]), hxhim_data_t::HXHIM_DATA_DOUBLE);

        req.subjects[i]       = ref;
        req.predicates[i]     = ReferenceBlob((void *) hist_names[0].data(),
                                          hist_names[0].size(),
                                          hxhim_data_t::HXHIM_DATA_BYTE);
        req.objects[i]        = ref;
        req.count++;

        req.subjects[i + 1]   = ref;
        req.predicates[i + 1] = ReferenceBlob((void *) hist_names[1].data(),
                                          hist_names[1].size(),
                                          hxhim_data_t::HXHIM_DATA_BYTE);
        req.objects[i + 1]    = ref;
        req.count++;
    }

    destruct(ds->operate(&req));
    delete [] values;

    datastore::Datastore::Histograms *hists = nullptr;
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
            datastore::Datastore::Histograms::const_iterator it = hists->find(hist_names[0]);
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
            datastore::Datastore::Histograms::const_iterator it = hists->find(hist_names[1]);
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
    EXPECT_EQ(ds->data().size(), triples + hist_count);

    // clear the histogram data
    for(datastore::Datastore::Histograms::value_type &hist : *hists) {
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
    EXPECT_EQ(ds->ReadHistograms(datastore::HistNames_t(std::begin(hist_names),
                                                        std::end  (hist_names))),
              hist_count);

    // Check that the recovered histograms have data
    {
        double *buckets = nullptr;
        std::size_t *counts = nullptr;
        std::size_t size = 0;

        // hist0 has buckets
        {
            datastore::Datastore::Histograms::const_iterator it = hists->find(hist_names[0]);
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
            datastore::Datastore::Histograms::const_iterator it = hists->find(hist_names[1]);
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
