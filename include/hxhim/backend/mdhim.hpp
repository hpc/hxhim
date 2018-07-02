#ifndef HXHIM_BACKEND_MDHIM_HPP
#define HXHIM_BACKEND_MDHIM_HPP

#include "base.hpp"
#include "mdhim/mdhim.hpp"
#include "transport/constants.h"

namespace hxhim {
namespace backend {

class mdhim : public base {
    public:
        mdhim(MPI_Comm comm, const std::string &config);
        ~mdhim();

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

    private:
        mdhim_t *md;
        mdhim_options_t *mdhim_opts;

        static const std::map<enum hxhim_get_op, enum TransportGetMessageOp> GET_OP_MAPPING;

        int Open(MPI_Comm comm, const std::string &config);
};

}
}

#endif
