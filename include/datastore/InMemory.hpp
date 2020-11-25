#ifndef HXHIM_DATASTORE_INMEMORY_HPP
#define HXHIM_DATASTORE_INMEMORY_HPP

#include "datastore/datastore.hpp"

namespace datastore {

class InMemory : public Datastore {
    public:
        struct Config : datastore::Config {
            Config()
                : ::datastore::Config(datastore::IN_MEMORY)
            {}
        };

        InMemory(const int rank,
                 const int id,
                 Transform::Callbacks *callbacks,
                 const std::string &name);

        virtual ~InMemory();

    private:
        bool OpenImpl(const std::string &); // NO OP
        void CloseImpl();

        Transport::Response::BPut    *BPutImpl   (Transport::Request::BPut    *req);
        Transport::Response::BGet    *BGetImpl   (Transport::Request::BGet    *req);
        Transport::Response::BGetOp  *BGetOpImpl (Transport::Request::BGetOp  *req);
        Transport::Response::BDelete *BDeleteImpl(Transport::Request::BDelete *req);

        int SyncImpl();

    protected:
        std::map<std::string, std::string> db;
};

}

#endif
