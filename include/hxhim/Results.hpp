#ifndef HXHIM_RESULTS_HPP
#define HXHIM_RESULTS_HPP

#include <cstddef>
#include <list>
#include <memory>

#include "hxhim/Results.h"
#include "hxhim/constants.h"
#include "hxhim/struct.h"
#include "utils/Blob.hpp"

namespace hxhim {

/**
 * Results
 * This structure holds a linked list of all results waiting to be used by a user.
 * A single result node contains exactly 1 set of data. Results of bulk operations
 * are flattened when they are stored.
 *
 * Each result takes ownership of the pointers passed into its constructor.
 *
 * Usage:
 *
 *     hxhim::Results *res = Flush(hx);
 *     for(res->GoToHead(); res->Valid(); res->GoToNext()) {
 *         switch (res->Curr()->GetType()) {
 *             case HXHIM_RESULT_PUT:
 *                 {
 *                     hxhim::Results::Put *put = static_cast<hxhim::Results::Put *>(res->Curr());
 *                     // do stuff with put
 *                 }
 *                 break;
 *             case HXHIM_RESULT_GET:
 *             case HXHIM_RESULT_GETOP:
 *                 {
 *                     hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(res->Curr());
 *                     // do stuff with get
 *                 }
 *                 break;
 *             case HXHIM_RESULT_DEL:
 *                 {
 *                     hxhim::Results::Delete *del = static_cast<hxhim::Results::Delete *>(res->Curr());
 *                     // do stuff with del
 *                 }
 *                 break;
 *             default:
 *                 break;
 *         }
 *     }
 *     hxhim_results_destroy(res);
 *
 */
class Results {
    public:
        struct Result {
            Result(hxhim_t *hx, const hxhim_result_type type,
                   const int datastore, const int status);
            virtual ~Result();

            hxhim_t *hx;
            hxhim_result_type_t type;

            int datastore;
            int status;
        };

        struct SubjectPredicate : public Result {
            SubjectPredicate(hxhim_t *hx, const hxhim_result_type type,
                             const int datastore, const int status);
            virtual ~SubjectPredicate();

            Blob *subject;
            Blob *predicate;
        };

        /** @description Convenience struct for PUT results */
        struct Put final : public SubjectPredicate {
            Put(hxhim_t *hx, const int datastore, const int status);
        };

    private:
        /** @description Base structure for GET results */
        template <hxhim_result_type_t gettype>
        struct GetBase final : public SubjectPredicate {
            GetBase(hxhim_t *hx, const int datastore, const int status)
                : SubjectPredicate(hx, gettype, datastore, status),
                  object_type(HXHIM_INVALID_TYPE),
                  object(nullptr),
                  next(nullptr)
            {}

            hxhim_type_t object_type;
            Blob *object;

            struct GetBase *next;
        };

    public:
        /** @description Convenience struct for GET results */
        typedef GetBase<hxhim_result_type_t::HXHIM_RESULT_GET> Get;

        /** @description Convenience struct for GETOP results */
        typedef GetBase<hxhim_result_type_t::HXHIM_RESULT_GETOP> GetOp;

        /** @description Convenience struct for DEL results */
        struct Delete final : public SubjectPredicate {
            Delete(hxhim_t *hx, const int datastore, const int status);
        };

        /** @description Convenience struct for SYNC results */
        struct Sync final : public Result {
            Sync(hxhim_t *hx, const int datastore, const int status);
        };

        /** @description Convenience struct for HISTOGRAM results */
        struct Histogram final : public Result {
            Histogram(hxhim_t *hx, const int datastore, const int status);

            double *buckets;
            std::size_t *counts;
            std::size_t size;
        };

    public:
        Results(hxhim_t *hx);
        ~Results();

        static void Destroy(Results *res);

        // Accessors (controls the "curr" pointer)
        bool Valid() const;
        Result *GoToHead();
        Result *GoToNext();
        Result *Curr() const;
        Result *Next() const;

        // Appends a single new node to the list
        Result *Add(Result *res);

        // Moves and appends another set of results; the list being appended is emptied out
        Results &Append(Results *other);

        std::size_t size() const;

    private:
        hxhim_t *hx;
        std::list <Result *> results;
        std::list <Result *>::iterator curr;
};

}

#endif
