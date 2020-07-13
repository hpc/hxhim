#ifndef HXHIM_RESULTS_HPP
#define HXHIM_RESULTS_HPP

#include <cstddef>
#include <list>
#include <memory>

#include "hxhim/Results.h"
#include "hxhim/constants.h"
#include "hxhim/struct.h"
#include "utils/Blob.hpp"
#include "utils/Histogram.hpp"

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
 *         hxhim_result_type_t type;
 *         res->Type(&type);
 *         switch (type) {
 *             case HXHIM_RESULT_PUT:
 *                 // do stuff with put
 *                 break;
 *             case HXHIM_RESULT_GET:
 *             case HXHIM_RESULT_GETOP:
 *                 // do stuff with get
 *                 break;
 *             case HXHIM_RESULT_DEL:
 *                 // do stuff with del
 *                 break;
 *             case HXHIM_RESULT_HISTOGRAM:
 *                 // do stuff with histogram
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

            struct GetBase<gettype> *next;
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
        struct Hist final : public Result {
            Hist(hxhim_t *hx, const int datastore, const int status);
            ~Hist();

            // owned by this struct
            ::Histogram::Histogram *hist;
        };

    public:
        Results(hxhim_t *hx);
        ~Results();

        static void Destroy(Results *res);

        // Control the "curr" pointer
        bool Valid() const;
        Result *GoToHead();
        Result *GoToNext();
        Result *Curr() const;

        // Appends a single new node to the list
        Result *Add(Result *res);

        // Moves and appends another set of results; the list being appended is emptied out
        void Append(Results *other);

        std::size_t Size() const;

        // Accessors
        // pointers are only valid if the Result they came from are still valid
        int Type(enum hxhim_result_type *type) const;
        int Status(int *status) const;
        int Datastore(int *datastore) const;
        int Subject(void **subject, size_t *subject_len) const;
        int Predicate(void **predicate, size_t *predicate_len) const;
        int ObjectType(enum hxhim_type_t *object_type) const;
        int Object(void **object, size_t *object_len) const;
        int Histogram(::Histogram::Histogram **hist) const; // C++ only
        int Histogram(double **buckets, std::size_t **counts, std::size_t *size) const;

    private:
        hxhim_t *hx;
        std::list <Result *> results;
        std::list <Result *>::iterator curr;
};

}

#endif
