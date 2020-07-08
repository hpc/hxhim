#ifndef HXHIM_DATASTORE_LEVELDB_HPP
#define HXHIM_DATASTORE_LEVELDB_HPP

#include <leveldb/db.h>

#include "datastore/datastore.hpp"

namespace hxhim {
namespace datastore {

class leveldb : public Datastore {
    public:
        leveldb(const int rank,
                Histogram::Histogram *hist,
                const std::string &exact_name,
                const bool create_if_missing);
        leveldb(const int rank,
                const int id,
                Histogram::Histogram *hist,
                const std::string &prefix,
                const std::string &name,
                const bool create_if_missing);
        virtual ~leveldb();

        const std::string &name() const;

    private:
        bool OpenImpl(const std::string &new_name);
        void CloseImpl();

        Transport::Response::BPut    *BPutImpl   (Transport::Request::BPut    *req);
        Transport::Response::BGet    *BGetImpl   (Transport::Request::BGet    *req);
        Transport::Response::BGetOp  *BGetOpImpl (Transport::Request::BGetOp  *req);

        /** NOTE: LevelDB returns success so long as one item */
        /** being deleted exists and was deleted successfully */
        Transport::Response::BDelete *BDeleteImpl(Transport::Request::BDelete *req);

        int SyncImpl();

    protected:
        std::string dbname;
        bool create_if_missing;

        ::leveldb::DB *db;
        ::leveldb::Options options;
};

}
}

#endif
