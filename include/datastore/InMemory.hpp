#ifndef HXHIM_DATASTORE_INMEMORY_HPP
#define HXHIM_DATASTORE_INMEMORY_HPP

#include <map>

#include <mpi.h>

#include "datastore/datastore.hpp"

namespace hxhim {
namespace datastore {

class InMemory : public Datastore {
    public:
        InMemory(hxhim_t *hx,
                 Histogram::Histogram *hist,
                 const std::string &exact_name);
        InMemory(hxhim_t *hx,
                 const int id,
                 Histogram::Histogram *hist,
                 const std::string &name);
        ~InMemory();

        int StatFlush();

    private:
        bool OpenImpl(const std::string &name_name);
        void CloseImpl();

        Transport::Response::BPut    *BPutImpl   (Transport::Request::BPut    *req);
        Transport::Response::BGet    *BGetImpl   (Transport::Request::BGet    *req);
        Transport::Response::BGet2   *BGetImpl2  (Transport::Request::BGet2   *req);
        Transport::Response::BGetOp  *BGetOpImpl (Transport::Request::BGetOp  *req);
        Transport::Response::BDelete *BDeleteImpl(Transport::Request::BDelete *req);

        int SyncImpl();

    private:
        int Open(MPI_Comm comm, const std::string &config);

        std::map<std::string, std::string> db;
};

}
}

#endif
