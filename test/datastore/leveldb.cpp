#include <cstring>
#include <sstream>

#include <gtest/gtest.h>
#include <leveldb/db.h>
#include <mpi.h>

#include "datastore/leveldb.hpp"
#include "hxhim/triplestore.hpp"
#include "rm_r.hpp"
#include "triples.hpp"
#include "utils/memory.hpp"

class LevelDBTest : public datastore::leveldb {
    public:
        LevelDBTest(const int rank, const std::string &name)
            : datastore::leveldb(rank, nullptr, nullptr, name, true)
        {}

        ~LevelDBTest()  {
            Close();
            cleanup();
        }

        ::leveldb::DB *data() const {
            return db;
        }

    private:
        void cleanup() {
            remove(dbname.c_str());
        }
};

// create a test LevelDB datastore and insert some triples
static LevelDBTest *setup() {
    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    std::stringstream s;
    s << "LEVELDB-TEST-" << rank;

    rm_r(s.str());

    LevelDBTest *ds = construct<LevelDBTest>(rank, s.str());

    Transport::Request::BPut req(count);

    for(std::size_t i = 0; i < count; i++) {
        req.subjects[i]   = BLOB(subjects[i]);
        req.predicates[i] = BLOB(predicates[i]);
        req.objects[i]    = BLOB(objects[i]);
        req.count++;
    }

    Transport::Response::BPut * res = ds->operate(&req);
    destruct(res);

    return ds;
}

TEST(LevelDB, BPut) {
    LevelDBTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    ::leveldb::DB *db = ds->data();

    // read directly from leveldb since setup() already did PUTs
    for(std::size_t i = 0; i < count; i++) {
        std::string key;
        EXPECT_EQ(sp_to_key(BLOB(subjects[i]), BLOB(predicates[i]), key), HXHIM_SUCCESS);
        EXPECT_NE(key.size(), 0);

        std::string value;
        leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
        EXPECT_EQ(status.ok(), true);
        EXPECT_EQ(memcmp(BLOB(objects[i]).data(), value.c_str(), value.size()), 0);
    }

    // make sure datastore only has count items
    std::size_t items = 0;
    leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
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
    Transport::Request::BGet req(count + 1);
    for(std::size_t i = 0; i < count; i++) {
        req.subjects[i]     = BLOB(subjects[i]);
        req.predicates[i]   = BLOB(predicates[i]);
        req.object_types[i] = BLOB(objects[i]).data_type();
        req.count++;
    }

    // non-existant subject-predicate pair
    req.subjects[count]     = BLOB(std::string("sub3"));
    req.predicates[count]   = BLOB(std::string("pred3"));
    req.object_types[count] = hxhim_data_t::HXHIM_DATA_BYTE;
    req.count++;

    Transport::Response::BGet *res = ds->operate(&req);
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->count, count + 1);
    for(std::size_t i = 0; i < count; i++) {
        EXPECT_EQ(res->statuses[i], DATASTORE_SUCCESS);
        ASSERT_NE(res->objects[i].data(), nullptr);
        EXPECT_EQ(res->objects[i].size(), BLOB(objects[i]).size());
        EXPECT_EQ(std::memcmp(BLOB(objects[i]).data(), res->objects[i].data(), res->objects[i].size()), 0);
    }

    EXPECT_EQ(res->statuses[count], DATASTORE_ERROR);
    EXPECT_EQ(res->objects[count].data(), nullptr);

    destruct(res);
    destruct(ds);
}

TEST(LevelDB, BGetOp) {
    LevelDBTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    for(int op = HXHIM_GETOP_EQ; op < HXHIM_GETOP_INVALID; op++) {
        Transport::Request::BGetOp req(1);
        req.subjects[0]     = BLOB(subjects[0]);
        req.predicates[0]   = BLOB(predicates[0]);
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

TEST(LevelDB, BDelete) {
    LevelDBTest *ds = setup();
    ASSERT_NE(ds, nullptr);

    ::leveldb::DB *db = ds->data();

    // delete the triples
    {
        Transport::Request::BDelete req(count + 1);
        for(std::size_t i = 0; i < count; i++) {
            req.subjects[i]     = BLOB(subjects[i]);
            req.predicates[i]   = BLOB(predicates[i]);
            req.count++;
        }

        // non-existant subject-predicate pair
        req.subjects[count]     = BLOB(std::string("sub3"));
        req.predicates[count]   = BLOB(std::string("pred3"));
        req.count++;

        Transport::Response::BDelete *res = ds->operate(&req);
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
        Transport::Request::BGet req(count + 1);
        for(std::size_t i = 0; i < count; i++) {
            req.subjects[i]     = BLOB(subjects[i]);
            req.predicates[i]   = BLOB(predicates[i]);
            req.count++;
        }

        // non-existant subject-predicate pair
        req.subjects[count]   = BLOB(std::string("sub3"));
        req.predicates[count] = BLOB(std::string("pred3"));
        req.count++;

        Transport::Response::BGet *res = ds->operate(&req);
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
            std::string key;
            EXPECT_EQ(sp_to_key(BLOB(subjects[i]), BLOB(predicates[i]), key), HXHIM_SUCCESS);
            EXPECT_NE(key.size(), 0);

            std::string value;
            leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
            EXPECT_EQ(status.ok(), false);
            EXPECT_EQ(memcmp(BLOB(objects[i]).data(), value.c_str(), value.size()), 0);
        }
    }

    // make sure datastore doesn't have the original SPO triples
    {
        std::size_t items = 0;
        leveldb::Iterator *it = db->NewIterator(leveldb::ReadOptions());
        for(it->SeekToFirst(); it->Valid(); it->Next()) {
            items++;
        }
        EXPECT_EQ(it->status().ok(), true);
        EXPECT_EQ(items, 0);
        delete it;
    }

    destruct(ds);
}
