#ifndef HXHIM_DATASTORE_INMEMORY_HPP
#define HXHIM_DATASTORE_INMEMORY_HPP

#include "datastore/datastore.hpp"

namespace Datastore {

class InMemory : public Datastore {
    public:
        struct Config : ::Datastore::Config {
            Config()
                : ::Datastore::Config(::Datastore::IN_MEMORY)
            {}
        };

        InMemory(const int rank,
                 const int id,
                 Transform::Callbacks *callbacks);

        virtual ~InMemory();
        bool Open();

    private:
        bool OpenImpl(const std::string &);
        void CloseImpl();
        bool UsableImpl() const;

        Message::Response::BPut    *BPutImpl   (Message::Request::BPut    *req);
        Message::Response::BGet    *BGetImpl   (Message::Request::BGet    *req);
        Message::Response::BGetOp  *BGetOpImpl (Message::Request::BGetOp  *req);
        Message::Response::BDelete *BDeleteImpl(Message::Request::BDelete *req);

        int WriteHistogramsImpl();
        std::size_t ReadHistogramsImpl(const HistNames_t &names);
        int SyncImpl();

    protected:
        bool good;
        std::map<std::string, std::string> db;
};

}

#endif
