#ifndef TEST_DATASTORE_HPP
#define TEST_DATASTORE_HPP

#include "datastore/datastore.hpp"

static const std::chrono::microseconds PUT_TIME(123);
static const std::chrono::microseconds GET_TIME(456);
static const uint64_t PUT_TIME_UINT64 = std::chrono::duration_cast<std::chrono::nanoseconds>(PUT_TIME).count();
static const uint64_t GET_TIME_UINT64 = std::chrono::duration_cast<std::chrono::nanoseconds>(GET_TIME).count();

class TestDatastore : public datastore::Datastore {
    public:
        TestDatastore(const int id, Histogram::Histogram *hist = nullptr)
            : datastore::Datastore(0, id, hist)
        {}

    private:
        bool OpenImpl(const std::string &) { return true; }
        void CloseImpl() {}

        Transport::Response::BPut *BPutImpl(Transport::Request::BPut *req) {
            datastore::Datastore::Stats::Event event;
            event.time.start = ::Stats::now();
            event.count = 1;
            event.time.end = event.time.start + PUT_TIME;
            stats.puts.emplace_back(event);

            Transport::Response::BPut *res = construct<Transport::Response::BPut>(req->count);
            res->count = req->count;
            for(std::size_t i = 0; i < req->count; i++) {
                res->statuses[i] = DATASTORE_SUCCESS;
            }
            return res;
        }

        Transport::Response::BGet *BGetImpl(Transport::Request::BGet *) {
            datastore::Datastore::Stats::Event event;
            event.time.start = ::Stats::now();
            event.count = 1;
            event.time.end = event.time.start + GET_TIME;
            stats.gets.emplace_back(event);

            return nullptr;
        }

        Transport::Response::BGetOp *BGetOpImpl(Transport::Request::BGetOp *) { return nullptr; }

        Transport::Response::BDelete *BDeleteImpl(Transport::Request::BDelete *) { return nullptr; }

        int SyncImpl() { return DATASTORE_SUCCESS; }
};

#endif
