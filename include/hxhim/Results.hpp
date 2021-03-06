#ifndef HXHIM_RESULTS_HPP
#define HXHIM_RESULTS_HPP

#include <cstddef>
#include <cstdint>
#include <memory>

#include "hxhim/Blob.hpp"
#include "hxhim/Results.h"
#include "hxhim/constants.h"
#include "hxhim/struct.h"
#include "utils/Histogram.hpp"
#include "utils/Stats.hpp"

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
 *     HXHIM_CXX_RESULTS_LOOP(res) {
 *         enum hxhim_op_t op;
 *         res->Op(&op);
 *         switch (op) {
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
 * Data should be extracted with their respective functions:
 *     void *subject = nullptr;
 *     std::size_t subject_len = 0;
 *     res->Subject(&subject, &subject_len);
 *
 *     void *predicate = nullptr;
 *     std::size_t predicate_len = 0;
 *     res->Predicate(&predicate, &predicate_len);
 *
 *     etc.
 *
 */

/** Convenience macro to loop over a hxhim::Results pointer */
#define HXHIM_CXX_RESULTS_LOOP(results)         \
    for((results)->GoToHead();                  \
        (results)->ValidIterator();             \
        (results)->GoToNext())

class Results {
    public:
        struct Result {
            Result(hxhim_t *hx, const enum hxhim_op_t op,
                   const int range_server, const int ds_status);
            virtual ~Result();

            hxhim_t *hx;
            enum hxhim_op_t op;

            int range_server;
            int status;

            // timestamps for a single operation
            // epoch can be obtained with hxhim::GetEpoch
            struct Timestamps {
                Timestamps();
                ~Timestamps();

                ::Stats::Chronostamp *alloc;
                struct ::Stats::Send send;
                struct ::Stats::SendRecv transport;
                struct ::Stats::Recv recv;
            };

            Timestamps timestamps;

            struct Result *next;
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

    private:
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

    public:
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

    public:
        Results();
        ~Results();

        static void Destroy(Results *res);

        // Appends a single new node to the list
        // timestamps are not updated
        Result *Add(Result *response);

        // Update duration separately
        uint64_t UpdateDuration(const uint64_t ns);

        // Moves and appends another set of results; the list being appended is emptied out
        // timestamps are updated
        void Append(Results *other);

        // Accessors for the entire list of results
        std::size_t Size() const;
        uint64_t Duration() const;

        // Control the "curr" pointer
        bool ValidIterator() const;
        Result *GoToHead();
        Result *GoToNext();

    private:
        Result *Curr() const;
        void push_back(Result *response);

    public:
        // Accessors for individual results
        // pointers are only valid if the Result they came from are still valid
        int Op(enum hxhim_op_t *op) const;
        int Status(int *status) const;
        int RangeServer(int *range_server) const;
        int Subject(void **subject, std::size_t *subject_len, hxhim_data_t *subject_type) const;
        int Predicate(void **predicate, std::size_t *predicate_len, hxhim_data_t *predicate_type) const;
        int Object(void **object, std::size_t *object_len, hxhim_data_t *object_type) const;
        int Histogram(const char **name, std::size_t *name_len, double **buckets, std::size_t **counts, std::size_t *size) const;

        // These functions are only available in C++
        int Histogram(::Histogram::Histogram **hist) const;
        int Timestamps(struct Result::Timestamps **timestamps) const;

    private:
        // list of results
        Result *head;
        Result *tail;
        Result *curr;
        std::size_t count;

        uint64_t duration;
};

}

#endif
