#ifndef HXHIM_RESULTS_HPP
#define HXHIM_RESULTS_HPP

#include <cstddef>
#include <list>
#include <memory>

#include "hxhim/Results.h"
#include "hxhim/constants.h"
#include "hxhim/struct.h"
#include "transport/Messages/Messages.hpp"
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
            virtual ~Result();

            hxhim_result_type_t type;

            int datastore;
            int status;
        };

        /** @description Convenience struct for PUT results */
        struct Put final : public Result {};

        /** @description Convenience struct for GET results    */
        struct Get final : public Result {
            Get();
            ~Get();

            Blob *subject;
            Blob *predicate;
            hxhim_type_t object_type;
            Blob *object;

            struct {
                void *subject;
                void *predicate;
                void *object;
                std::size_t *object_len;
            } orig;
        };

        /** @description Convenience struct for DEL results */
        struct Delete final : public Result {};

        /** @description Convenience struct for SYNC results */
        struct Sync final : public Result {};

        /** @description Convenience struct for HISTOGRAM results */
        struct Histogram final : public Result {
            double *buckets;
            std::size_t *counts;
            std::size_t size;
        };

    public:
        Results();
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
        std::list <Result *> results;
        std::list <Result *>::iterator curr;
};

}

#endif
