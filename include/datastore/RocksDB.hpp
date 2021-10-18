#ifndef HXHIM_DATASTORE_ROCKSDB_HPP
#define HXHIM_DATASTORE_ROCKSDB_HPP

#include <rocksdb/db.h>

#include "datastore/datastore.hpp"

namespace Datastore {

class RocksDB : public Datastore {
    public:
        struct Config : ::Datastore::Config {
            Config(const std::string &prefix,
                   const std::string &postfix,
                   const bool create_if_missing)
                : ::Datastore::Config(::Datastore::ROCKSDB),
                  prefix(prefix),
                  postfix(postfix),
                  create_if_missing(create_if_missing)
            {}

            const std::string prefix;
            const std::string postfix;
            const bool create_if_missing;
        };

        RocksDB(const int rank,
                const int id,
                Transform::Callbacks *callbacks,
                const bool create_if_missing);
        virtual ~RocksDB();

        const std::string &Name() const;

    private:
        bool OpenImpl(const std::string &new_name);
        void CloseImpl();
        bool UsableImpl() const;

        Transport::Response::BPut    *BPutImpl   (Transport::Request::BPut    *req);
        Transport::Response::BGet    *BGetImpl   (Transport::Request::BGet    *req);
        Transport::Response::BGetOp  *BGetOpImpl (Transport::Request::BGetOp  *req);

        /** NOTE: rocksdb returns success so long as one item */
        /** being deleted exists and was deleted successfully */
        Transport::Response::BDelete *BDeleteImpl(Transport::Request::BDelete *req);

        int WriteHistogramsImpl();
        std::size_t ReadHistogramsImpl(const HistNames_t &names);
        int SyncImpl();

    protected:
        std::string name;
        bool create_if_missing;

        ::rocksdb::DB *db;
        ::rocksdb::Options options;
};

}

#endif
