#ifndef HXHIM_DATASTORE_INMEMORY_HPP
#define HXHIM_DATASTORE_INMEMORY_HPP

#include <map>

#include <mpi.h>

#include "datastore/datastore.hpp"
#include "hxhim/struct.h"

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

        Transport::Response::BPut *BPutImpl(void **subjects, std::size_t *subject_lens,
                                            void **predicates, std::size_t *predicate_lens,
                                            hxhim_type_t *object_types, void **objects, std::size_t *object_lens,
                                            std::size_t count);
        Transport::Response::BGet *BGetImpl(void **subjects, std::size_t *subject_lens,
                                            void **predicates, std::size_t *predicate_lens,
                                            hxhim_type_t *object_types,
                                            std::size_t count);
        Transport::Response::BGet2 *BGetImpl2(void ***subjects, std::size_t **subject_lens,
                                              void ***predicates, std::size_t **predicate_lens,
                                              hxhim_type_t **object_types, void ***objects, std::size_t ***object_lens,
                                              std::size_t count);
        Transport::Response::BGetOp *BGetOpImpl(void *subject, std::size_t subject_len,
                                                void *predicate, std::size_t predicate_len,
                                                hxhim_type_t object_type,
                                                std::size_t recs, enum hxhim_get_op_t op);
        Transport::Response::BDelete *BDeleteImpl(void **subjects, std::size_t *subject_lens,
                                                  void **predicates, std::size_t *predicate_lens,
                                                  std::size_t count);

        int SyncImpl();

    private:
        int Open(MPI_Comm comm, const std::string &config);

        std::map<std::string, std::string> db;
};

}
}

#endif
