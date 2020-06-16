#ifndef HXHIM_DATASTORE_LEVELDB_HPP
#define HXHIM_DATASTORE_LEVELDB_HPP

#include <leveldb/db.h>

#include "datastore/datastore.hpp"

namespace hxhim {
namespace datastore {

class leveldb : public Datastore {
    public:
        leveldb(hxhim_t *hx,
                Histogram::Histogram *hist,
                const std::string &exact_name);
        leveldb(hxhim_t *hx,
                const int id,
                Histogram::Histogram *hist,
                const std::string &name, const bool create_if_missing);
        ~leveldb();

        int StatFlush();

    private:
        bool OpenImpl(const std::string &new_name);
        void CloseImpl();

        Transport::Response::BPut    *BPutImpl   (Transport::Request::BPut    *req);
        Transport::Response::BGet    *BGetImpl   (Transport::Request::BGet    *req);
        Transport::Response::BGetOp  *BGetOpImpl (Transport::Request::BGetOp  *req);
        Transport::Response::BDelete *BDeleteImpl(Transport::Request::BDelete *req);

        int SyncImpl();

    private:
        bool create_if_missing;

        ::leveldb::DB *db;
        ::leveldb::Options options;
};

}
}

#endif
