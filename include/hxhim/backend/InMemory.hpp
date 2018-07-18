#ifndef HXHIM_BACKEND_INMEMORY_HPP
#define HXHIM_BACKEND_INMEMORY_HPP

#include <map>

#include <mpi.h>

#include "base.hpp"
#include "hxhim/struct.h"

namespace hxhim {
namespace backend {

class InMemory : public base {
    public:
        InMemory(hxhim_t *hx);
        ~InMemory();

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
        int Open(MPI_Comm comm, const std::string &config);

        std::map<std::string, std::string> db;

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
