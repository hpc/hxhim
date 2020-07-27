#ifndef HXHIM_RESULTS_HPP
#define HXHIM_RESULTS_HPP

#include <cstddef>
#include <deque>
#include <memory>

#include "hxhim/Results.h"
#include "hxhim/constants.h"
#include "hxhim/struct.h"
#include "utils/Blob.hpp"
#include "utils/Histogram.hpp"
#include "utils/Stats.hpp"
#include "utils/memory.hpp"

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
 *     for(res->GoToHead(); res->ValidIterator(); res->GoToNext()) {
 *         enum hxhim_op_t op;
 *         res->Op(&op);
 *         switch (type) {
 *             case HXHIM_PUT:
 *                 // do stuff with put
 *                 break;
 *             case HXHIM_GET:
 *             case HXHIM_GETOP:
 *                 // do stuff with get
 *                 break;
 *             case HXHIM_DELETE:
 *                 // do stuff with del
 *                 break;
 *             case HXHIM_HISTOGRAM:
 *                 // do stuff with hist
 *                 break;
 *             default:
 *                 break;
 *         }
 *     }
 *     hxhim::Results::Destroy(res);
 *
 * There are three ways to extract data from individual results.
 * The recommended method is to use the hxhim::Results interface:
 *
 *     void *      subject     = nullptr;
 *     std::size_t subject_len = 0;
 *     res->Subject(&subject, &subject_len);
 *
 * The data can also be extracted by casting the current result to the
 * appropriate subtype and using Blob::get to get the pointers:
 *
 *     hxhim::Results::Put *put = static_cast<hxhim::Results::Put *>(res->Curr());
 *     void *      subject     = nullptr;
 *     std::size_T subject_len = 0;
 *     put->subject->get(&subject, &subject_len);
 *
 * Instead of using Blob::get, Blob::data and Blob::size can also be
 * used to retreive the values:
 *
 *     hxhim::Results::Put *put = static_cast<hxhim::Results::Put *>(res->Curr());
 *     void *      subject     = put->subject->data();
 *     std::size_t subject_len = put->subject->len();
 *
 */

class Results {
    public:
        struct Result {
            Result(hxhim_t *hx, const enum hxhim_op_t op,
                   const int datastore, const int status);
            virtual ~Result();

            hxhim_t *hx;
            enum hxhim_op_t op;

            int datastore;
            int status;

            // timestamps for a single operation
            // epoch can be obtained with hxhim::GetEpoch
            struct Timestamps {
                struct ::Stats::Send send;
                struct ::Stats::SendRecv transport;
                struct ::Stats::Recv recv;
            };

            Timestamps timestamps;
        };

        struct SubjectPredicate : public Result {
            SubjectPredicate(hxhim_t *hx, const enum hxhim_op_t type,
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
        template <enum hxhim_op_t gettype>
        struct GetBase final : public SubjectPredicate {
            GetBase(hxhim_t *hx, const int datastore, const int status)
                : SubjectPredicate(hx, gettype, datastore, status),
                  object_type(hxhim_object_type_t::HXHIM_OBJECT_TYPE_INVALID),
                  object(nullptr),
                  next(nullptr)
            {}

            ~GetBase() {
                destruct(object);
            }

            enum hxhim_object_type_t object_type;
            Blob *object;

            struct GetBase<gettype> *next;
        };

    public:
        /** @description Convenience struct for GET results */
        typedef GetBase<hxhim_op_t::HXHIM_GET> Get;

        /** @description Convenience struct for GETOP results */
        typedef GetBase<hxhim_op_t::HXHIM_GETOP> GetOp;

        /** @description Convenience struct for DEL results */
        struct Delete final : public SubjectPredicate {
            Delete(hxhim_t *hx, const int datastore, const int status);
        };

        /** @description Convenience struct for SYNC results */
        struct Sync final : public Result {
            Sync(hxhim_t *hx, const int datastore, const int status);
        };

        /** @description Convenience struct for DEL results */
        struct Hist final : public Result {
            Hist(hxhim_t *hx, const int datastore, const int status);

            std::shared_ptr<::Histogram::Histogram> histogram;
        };

    public:
        Results(hxhim_t *hx);
        ~Results();

        static void Destroy(Results *res);

        // Appends a single new node to the list
        // timestamps are not updated
        Result *Add(Result *response);

        // Update duration separately
        long double UpdateDuration(const long double dt);

        // Moves and appends another set of results; the list being appended is emptied out
        // timestamps are updated
        void Append(Results *other);

        // Accessors for the entire list of results
        std::size_t Size() const;
        long double Duration() const;

        // Control the "curr" pointer
        bool ValidIterator() const;
        Result *GoToHead();
        Result *GoToNext();
        Result *Curr() const;

        // Accessors for individual results
        // pointers are only valid if the Result they came from are still valid
        int Op(enum hxhim_op_t *op) const;
        int Status(int *status) const;
        int Datastore(int *datastore) const;
        int Subject(void **subject, std::size_t *subject_len) const;
        int Predicate(void **predicate, std::size_t *predicate_len) const;
        int ObjectType(enum hxhim_object_type_t *object_type) const;
        int Object(void **object, std::size_t *object_len) const;
        int Histogram(double **buckets, std::size_t **counts, std::size_t *size) const;

        // These functions are only available in C++
        int Histogram(::Histogram::Histogram **hist) const;
        int Timestamps(struct Result::Timestamps **timestamps) const;

    private:
        hxhim_t *hx;
        std::deque <Result *> results;
        decltype(results)::const_iterator curr;
        long double duration;
};

}

#endif
