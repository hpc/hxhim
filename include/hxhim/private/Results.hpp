#ifndef HXHIM_RESULTS_PRIVATE_HPP
#define HXHIM_RESULTS_PRIVATE_HPP

#include "hxhim/Results.hpp"
#include "hxhim/struct.h"
#include "message/Messages.hpp"

namespace hxhim {
    namespace Result {
        struct Result {
            Result(hxhim_t *hx, const enum hxhim_op_t op,
                   const int range_server, const int ds_status);
            virtual ~Result();

            hxhim_t *hx;
            enum hxhim_op_t op;

            int range_server;
            int status;

            Timestamps timestamps;
        };

        struct SubjectPredicate : public Result {
            SubjectPredicate(hxhim_t *hx, const enum hxhim_op_t type,
                             const int range_server, const int status);
            virtual ~SubjectPredicate();

            Blob subject;
            Blob predicate;
        };

        /** @description Convenience struct for PUT results */
        struct Put final : public SubjectPredicate {
            Put(hxhim_t *hx, const int range_server, const int status);
        };

        /** @description Base structure for GET results */
        template <enum hxhim_op_t gettype>
        struct GetBase final : public SubjectPredicate {
            GetBase(hxhim_t *hx, const int range_server, const int status)
                : SubjectPredicate(hx, gettype, range_server, status),
                  object(),
                  next(nullptr)
            {}

            Blob object;

            struct GetBase<gettype> *next;
        };

        /** @description Convenience struct for GET results */
        typedef GetBase<hxhim_op_t::HXHIM_GET> Get;

        /** @description Convenience struct for GETOP results */
        typedef GetBase<hxhim_op_t::HXHIM_GETOP> GetOp;

        /** @description Convenience struct for DEL results */
        struct Delete final : public SubjectPredicate {
            Delete(hxhim_t *hx, const int range_server, const int status);
        };

        /** @description Convenience struct for SYNC results */
        struct Sync final : public Result {
            Sync(hxhim_t *hx, const int range_server, const int status);
        };

        /** @description Convenience struct for DEL results */
        struct Hist final : public Result {
            Hist(hxhim_t *hx, const int range_server, const int status);

            std::shared_ptr<::Histogram::Histogram> histogram;
        };

        Result *init(hxhim_t *hx, Message::Response::Response *res,     const std::size_t i);
        Put    *init(hxhim_t *hx, Message::Response::BPut *bput,        const std::size_t i);
        Get    *init(hxhim_t *hx, Message::Response::BGet *bget,        const std::size_t i);
        GetOp  *init(hxhim_t *hx, Message::Response::BGetOp *bgetop,    const std::size_t i);
        Delete *init(hxhim_t *hx, Message::Response::BDelete *bdel,     const std::size_t i);
        Sync   *init(hxhim_t *hx, const int synced);
        Hist   *init(hxhim_t *hx, Message::Response::BHistogram *bhist, const std::size_t i);

        // add all responses into results with one call
        void AddAll(hxhim_t *hx, hxhim::Results *results, Message::Response::Response *response);
    }
}

extern "C"
{

typedef struct hxhim_results {
    hxhim_t *hx;
    hxhim::Results *res;
} hxhim_results_t;

}

/* wraps a hxhim::Results with a hxhim_results_t */
hxhim_results_t *hxhim_results_init(hxhim_t *hx, hxhim::Results *res);

#endif
