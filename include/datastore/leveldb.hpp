#if HXHIM_HAVE_LEVELDB

#ifndef HXHIM_DATASTORE_LEVELDB_HPP
#define HXHIM_DATASTORE_LEVELDB_HPP

#include <leveldb/db.h>
#include <mpi.h>

#include "datastore/datastore.hpp"
#include "hxhim/struct.h"

namespace hxhim {
namespace datastore {

class leveldb : public Datastore {
    public:
        leveldb(hxhim_t *hx,
                const std::size_t use_first_n, const HistogramBucketGenerator_t &generator, void *extra_args,
                const std::string &exact_name);
        leveldb(hxhim_t *hx,
                const int id,
                const std::size_t use_first_n, const HistogramBucketGenerator_t &generator, void *extra_args,
                const std::string &name, const bool create_if_missing);
        ~leveldb();

        void Close();
        int StatFlush();

    private:
        Transport::Response::BPut *BPutImpl(void **subjects, std::size_t *subject_lens,
                                            void **predicates, std::size_t *predicate_lens,
                                            hxhim_type_t *object_types, void **objects, std::size_t *object_lens,
                                            std::size_t count);
        Transport::Response::BGet *BGetImpl(void **subjects, std::size_t *subject_lens,
                                            void **predicates, std::size_t *predicate_lens,
                                            hxhim_type_t *object_types,
                                            std::size_t count);
        Transport::Response::BGetOp *BGetOpImpl(void *subject, std::size_t subject_len,
                                                void *predicate, std::size_t predicate_len,
                                                hxhim_type_t object_type,
                                                std::size_t recs, enum hxhim_get_op_t op);
        Transport::Response::BDelete *BDeleteImpl(void **subjects, std::size_t *subject_lens,
                                                  void **predicates, std::size_t *predicate_lens,
                                                  std::size_t count);

        int SyncImpl();

    public:
        std::ostream &print_config(std::ostream &stream) const;

    private:
        const std::string name;
        const bool create_if_missing;

        ::leveldb::DB *db;
        ::leveldb::Options options;
};

}
}

#endif

#endif
