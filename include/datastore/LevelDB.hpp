#ifndef HXHIM_DATASTORE_LEVELDB_HPP
#define HXHIM_DATASTORE_LEVELDB_HPP

#include <leveldb/db.h>

#include "datastore/datastore.hpp"

namespace Datastore {

class LevelDB : public Datastore {
    public:
        struct Config : ::Datastore::Config {
            Config(const std::string &prefix,
                   const std::string &postfix,
                   const bool create_if_missing)
                : ::Datastore::Config(::Datastore::LEVELDB),
                  prefix(prefix),
                  postfix(postfix),
                  create_if_missing(create_if_missing)
            {}

            const std::string prefix;
            const std::string postfix;
            const bool create_if_missing;
        };

        LevelDB(const int rank,
                const int id,
                Transform::Callbacks *callbacks,
                const bool create_if_missing);
        virtual ~LevelDB();

        const std::string &Name() const;

    private:
        bool OpenImpl(const std::string &new_name);
        void CloseImpl();
        bool UsableImpl() const;

        Message::Response::BPut    *BPutImpl   (Message::Request::BPut    *req);
        Message::Response::BGet    *BGetImpl   (Message::Request::BGet    *req);
        Message::Response::BGetOp  *BGetOpImpl (Message::Request::BGetOp  *req);

        /** NOTE: LevelDB returns success so long as one item */
        /** being deleted exists and was deleted successfully */
        Message::Response::BDelete *BDeleteImpl(Message::Request::BDelete *req);

        int WriteHistogramsImpl();
        std::size_t ReadHistogramsImpl(const HistNames_t &names);
        int SyncImpl();

    protected:
        std::string name;
        bool create_if_missing;

        ::leveldb::DB *db;
        ::leveldb::Options options;
};

}

#endif
