#ifndef HXHIM_BACKEND_BASE_HPP
#define HXHIM_BACKEND_BASE_HPP

#include "hxhim/Results.hpp"

namespace hxhim {
namespace backend {

/**
 * base
 * The base class for all backends used by HXHIM
 */
class base {
    public:
        virtual ~base() {}

        virtual void Close() {}
        virtual int Commit() = 0;
        virtual int StatFlush() = 0;
        virtual int GetStats(const int rank,
                             const bool get_put_times, long double *put_times,
                             const bool get_num_puts, std::size_t *num_puts,
                             const bool get_get_times, long double *get_times,
                             const bool get_num_gets, std::size_t *num_gets) = 0;

        virtual Results *BPut(void **subjects, std::size_t *subject_lens,
                              void **predicates, std::size_t *predicate_lens,
                              void **objects, std::size_t *object_lens,
                              std::size_t count) = 0;
        virtual Results *BGet(void **subjects, std::size_t *subject_lens,
                              void **predicates, std::size_t *predicate_lens,
                              std::size_t count) = 0;
        virtual Results *BGetOp(void *subject, std::size_t subject_len,
                                void *predicate, std::size_t predicate_len,
                                std::size_t count, enum hxhim_get_op op) = 0;
        virtual Results *BDelete(void **subjects, std::size_t *subject_lens,
                                 void **predicates, std::size_t *predicate_lens,
                                 std::size_t count) = 0;
};

}
}

#endif
