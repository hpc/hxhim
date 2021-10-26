#ifndef TEST_DATASTORE_HPP
#define TEST_DATASTORE_HPP

#include <map>

#include "datastore/datastore.hpp"

const std::chrono::microseconds PUT_TIME(123);
const std::chrono::microseconds GET_TIME(456);
const uint64_t PUT_TIME_UINT64 = std::chrono::duration_cast<std::chrono::nanoseconds>(PUT_TIME).count();
const uint64_t GET_TIME_UINT64 = std::chrono::duration_cast<std::chrono::nanoseconds>(GET_TIME).count();

class TestDatastore : public Datastore::Datastore {
    public:
        TestDatastore(const int id)
            : Datastore::Datastore(0, id, nullptr)
        {}

    private:
        bool OpenImpl(const std::string &) { return true; }
        void CloseImpl() {}
        bool UsableImpl() const {return true; }

        Message::Response::BPut *BPutImpl(Message::Request::BPut *req) {
            Datastore::Datastore::Stats::Event event;
            event.time.start = ::Stats::now();
            event.count = 1;
            event.time.end = event.time.start + PUT_TIME;
            stats.puts.emplace_back(event);

            Message::Response::BPut *res = construct<Message::Response::BPut>(req->count);
            res->count = req->count;
            for(std::size_t i = 0; i < req->count; i++) {
                res->statuses[i] = DATASTORE_SUCCESS;
            }
            return res;
        }

        Message::Response::BGet *BGetImpl(Message::Request::BGet *) {
            Datastore::Datastore::Stats::Event event;
            event.time.start = ::Stats::now();
            event.count = 1;
            event.time.end = event.time.start + GET_TIME;
            stats.gets.emplace_back(event);

            return nullptr;
        }

        Message::Response::BGetOp *BGetOpImpl(Message::Request::BGetOp *) { return nullptr; }

        Message::Response::BDelete *BDeleteImpl(Message::Request::BDelete *) { return nullptr; }

        int WriteHistogramsImpl() { return DATASTORE_SUCCESS; }
        std::size_t ReadHistogramsImpl(const ::Datastore::HistNames_t &) { return 0; }
        int SyncImpl() { return DATASTORE_SUCCESS; }
};

#endif
