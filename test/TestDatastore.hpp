#ifndef TEST_DATASTORE_HPP
#define TEST_DATSTORE_HPP

#include "datastore/datastore.hpp"

static const std::chrono::microseconds PUT_TIME(123);
static const std::chrono::microseconds GET_TIME(456);
static const double PUT_TIME_DOUBLE = std::chrono::duration_cast<std::chrono::nanoseconds>(PUT_TIME).count();
static const double GET_TIME_DOUBLE = std::chrono::duration_cast<std::chrono::nanoseconds>(GET_TIME).count();

class TestDatastore : public hxhim::datastore::Datastore {
    public:
        TestDatastore(const int id)
            : hxhim::datastore::Datastore(0, id, nullptr)
        {}

    private:
        bool OpenImpl(const std::string &) { return true; }
        void CloseImpl() {}

        Transport::Response::BPut *BPutImpl(Transport::Request::BPut *) {
            hxhim::datastore::Datastore::Stats::Event event;
            event.time.start = now();
            event.count = 1;
            event.time.end = event.time.start + PUT_TIME;
            stats.puts.emplace_back(event);

            return nullptr;
        }

        Transport::Response::BGet *BGetImpl(Transport::Request::BGet *) {
            hxhim::datastore::Datastore::Stats::Event event;
            event.time.start = now();
            event.count = 1;
            event.time.end = event.time.start + GET_TIME;
            stats.gets.emplace_back(event);

            return nullptr;
        }

        Transport::Response::BGetOp *BGetOpImpl (Transport::Request::BGetOp *) { return nullptr; }

        Transport::Response::BDelete *BDeleteImpl(Transport::Request::BDelete *) { return nullptr; }

        int SyncImpl() { return HXHIM_SUCCESS; }
};

#endif