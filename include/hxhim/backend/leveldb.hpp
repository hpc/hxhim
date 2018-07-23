#ifndef HXHIM_BACKEND_LEVELDB_HPP
#define HXHIM_BACKEND_LEVELDB_HPP

#include <leveldb/db.h>
#include <mpi.h>

#include "hxhim/struct.h"
#include "base.hpp"

namespace hxhim {
namespace backend {

class leveldb : public base {
    public:
        leveldb(hxhim_t *hx, const std::string &name, const bool create_if_missing = true);
        ~leveldb();

        void Close();
        int Commit();
        int StatFlush();
        int GetStats(const int rank,
                     const bool get_put_times, long double *put_times,
                     const bool get_num_puts, std::size_t *num_puts,
                     const bool get_get_times, long double *get_times,
                     const bool get_num_gets, std::size_t *num_gets);

        Results *BPut(void **subjects, std::size_t *subject_lens,
                      void **predicates, std::size_t *predicate_lens,
                      void **objects, std::size_t *object_lens,
                      std::size_t count);
        Results *BGet(void **subjects, std::size_t *subject_lens,
                      void **predicates, std::size_t *predicate_lens,
                      std::size_t count);
        Results *BGetOp(void *subject, std::size_t subject_len,
                        void *predicate, std::size_t predicate_len,
                        std::size_t count, enum hxhim_get_op op);
        Results *BDelete(void **subjects, std::size_t *subject_lens,
                         void **predicates, std::size_t *predicate_lens,
                         std::size_t count);

        std::ostream &print_config(std::ostream &stream) const;

    private:
        class GetResult : public Results::Get {
            public:
                GetResult(const int db, const ::leveldb::Iterator *it);
                ~GetResult();

                int GetSubject(void **subject, std::size_t *subject_len) const;
                int GetPredicate(void **predicate, std::size_t *predicate_len) const;
                int GetObject(void **object, std::size_t *object_len) const;

            private:
                const ::leveldb::Iterator *res;
        };

        struct GetOpResult : public Results::Get {
            public:
                GetOpResult(const bool ok, const int db, const ::leveldb::Slice &key, const ::leveldb::Slice &value);
                ~GetOpResult();

                int GetSubject(void **subject, std::size_t *subject_len) const;
                int GetPredicate(void **predicate, std::size_t *predicate_len) const;
                int GetObject(void **object, std::size_t *object_len) const;

            private:
                const ::leveldb::Slice k;
                const ::leveldb::Slice v;
        };

        const std::string name;
        const bool create_if_missing;

        ::leveldb::DB *db;
        ::leveldb::Options options;

        struct {
            std::size_t puts;
            long double put_times;
            std::size_t gets;
            long double get_times;
        } stats;
};

}
}

#endif
