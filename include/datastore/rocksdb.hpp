#ifndef HXHIM_DATASTORE_ROCKSDB_HPP
#define HXHIM_DATASTORE_ROCKSDB_HPP

#include <rocksdb/db.h>

#include "datastore/datastore.hpp"

namespace hxhim {
namespace datastore {

class rocksdb : public Datastore {
    public:
        struct Config : datastore::Config {
            Config()
                : ::hxhim::datastore::Config(hxhim::datastore::ROCKSDB)
            {}

            std::size_t id;
            std::string prefix;
            bool create_if_missing;
        };

        rocksdb(const int rank,
                Histogram::Histogram *hist,
                const std::string &exact_name,
                const bool create_if_missing);
        rocksdb(const int rank,
                const int offset,
                const int id,
                Histogram::Histogram *hist,
                const std::string &prefix,
                const std::string &name,
                const bool create_if_missing);
        virtual ~rocksdb();

        const std::string &name() const;

    private:
        bool OpenImpl(const std::string &new_name);
        void CloseImpl();

        Transport::Response::BPut    *BPutImpl   (Transport::Request::BPut    *req);
        Transport::Response::BGet    *BGetImpl   (Transport::Request::BGet    *req);
        Transport::Response::BGetOp  *BGetOpImpl (Transport::Request::BGetOp  *req);

        /** NOTE: rocksdb returns success so long as one item */
        /** being deleted exists and was deleted successfully */
        Transport::Response::BDelete *BDeleteImpl(Transport::Request::BDelete *req);

        int SyncImpl();

    protected:
        std::string dbname;
        bool create_if_missing;

        ::rocksdb::DB *db;
        ::rocksdb::Options options;
};

}
}

#endif
