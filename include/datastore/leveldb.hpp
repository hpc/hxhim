#ifndef HXHIM_DATASTORE_LEVELDB_HPP
#define HXHIM_DATASTORE_LEVELDB_HPP

#include <leveldb/db.h>

#include "datastore/datastore.hpp"

namespace datastore {

class leveldb : public Datastore {
    public:
        struct Config : datastore::Config {
            Config(const std::string &prefix,
                   const bool create_if_missing)
                : ::datastore::Config(datastore::LEVELDB),
                  prefix(prefix),
                  create_if_missing(create_if_missing)
            {}

            const std::string prefix;
            const bool create_if_missing;
        };

        leveldb(const int rank,
                const int id,
                Transform::Callbacks *callbacks,
                const bool create_if_missing);
        virtual ~leveldb();

        const std::string &Name() const;

    private:
        bool OpenImpl(const std::string &new_name);
        void CloseImpl();
        bool UsableImpl() const;

        Transport::Response::BPut    *BPutImpl   (Transport::Request::BPut    *req);
        Transport::Response::BGet    *BGetImpl   (Transport::Request::BGet    *req);
        Transport::Response::BGetOp  *BGetOpImpl (Transport::Request::BGetOp  *req);

        /** NOTE: LevelDB returns success so long as one item */
        /** being deleted exists and was deleted successfully */
        Transport::Response::BDelete *BDeleteImpl(Transport::Request::BDelete *req);

        int WriteHistogramsImpl();
        std::size_t ReadHistogramsImpl(const datastore::HistNames_t &names);
        int SyncImpl();

    protected:
        std::string name;
        bool create_if_missing;

        ::leveldb::DB *db;
        ::leveldb::Options options;
};

}

#endif
