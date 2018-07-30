#ifndef HXHIM_BACKEND_BASE_HPP
#define HXHIM_BACKEND_BASE_HPP

#include <mutex>

#include "hxhim/struct.h"
#include "transport/Messages.hpp"
#include "utils/Histogram.hpp"
#include "utils/elapsed.h"

namespace hxhim {
namespace backend {

/**
 * base
 * The base class for all backends used by HXHIM
 */
class base {
    public:
        base(hxhim_t *hx,
             const int id,
             const std::size_t use_first_n, const Histogram::BucketGen::generator &generator, void *extra_args);
        virtual ~base();

        virtual void Close() {}
        virtual int Commit() = 0;
        virtual int StatFlush() = 0;
        int GetStats(const int rank,
                     const bool get_put_times, long double *put_times,
                     const bool get_num_puts, std::size_t *num_puts,
                     const bool get_get_times, long double *get_times,
                     const bool get_num_gets, std::size_t *num_gets);

        Transport::Response::BPut *BPut(void **subjects, std::size_t *subject_lens,
                                        void **predicates, std::size_t *predicate_lens,
                                        hxhim_type_t *object_types, void **objects, std::size_t *object_lens,
                                        std::size_t count);
        Transport::Response::BGet *BGet(void **subjects, std::size_t *subject_lens,
                                        void **predicates, std::size_t *predicate_lens,
                                        hxhim_type_t *object_types,
                                        std::size_t count);
        Transport::Response::BGetOp *BGetOp(void *subject, std::size_t subject_len,
                                            void *predicate, std::size_t predicate_len,
                                            hxhim_type_t object_type,
                                            std::size_t recs, enum hxhim_get_op_t op);
        Transport::Response::BDelete *BDelete(void **subjects, std::size_t *subject_lens,
                                              void **predicates, std::size_t *predicate_lens,
                                              std::size_t count);

        virtual std::ostream &print_config(std::ostream &stream) const = 0;

    protected:
        virtual Transport::Response::BPut *BPutImpl(void **subjects, std::size_t *subject_lens,
                                                    void **predicates, std::size_t *predicate_lens,
                                                    hxhim_type_t *object_types, void **objects, std::size_t *object_lens,
                                                    std::size_t count) = 0;
        virtual Transport::Response::BGet *BGetImpl(void **subjects, std::size_t *subject_lens,
                                                    void **predicates, std::size_t *predicate_lens,
                                                    hxhim_type_t *object_types,
                                                    std::size_t count) = 0;
        virtual Transport::Response::BGetOp *BGetOpImpl(void *subject, std::size_t subject_len,
                                                        void *predicate, std::size_t predicate_len,
                                                        hxhim_type_t object_type,
                                                        std::size_t recs, enum hxhim_get_op_t op) = 0;
        virtual Transport::Response::BDelete *BDeleteImpl(void **subjects, std::size_t *subject_lens,
                                                          void **predicates, std::size_t *predicate_lens,
                                                          std::size_t count) = 0;

        int encode(const hxhim_type_t type, void *&ptr, std::size_t &len, bool &copied);
        int decode(const hxhim_type_t type, void *src, const std::size_t &src_len, void **dst, std::size_t *dst_len);

        hxhim_t *hx;
        const int id;
        Histogram::Histogram hist;

        std::mutex mutex;

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
