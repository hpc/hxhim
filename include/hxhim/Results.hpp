#ifndef HXHIM_RESULTS_HPP
#define HXHIM_RESULTS_HPP

#include <cstddef>
#include <list>
#include <map>
#include <memory>

#include "hxhim/Results.h"
#include "hxhim/constants.h"
#include "hxhim/struct.h"
#include "transport/Messages/Messages.hpp"
#include "utils/FixedBufferPool.hpp"

namespace hxhim {

/**
 * Results
 * This structure holds a linked list of all results waiting to be used by a user.
 * A single result node contains exactly 1 set of data. Results of bulk operations
 * are flattened when they are stored.
 *
 * Each result takes ownership of the pointers passed into the constructor.
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

            FixedBufferPool *buffers;
        };

        /** @description Convenience struct for PUT results */
        struct Put : public Result {};

        /** @description Convenience struct for GET results */
        struct Get : public Result {
            ~Get();

            void *subject;
            std::size_t subject_len;

            void *predicate;
            std::size_t predicate_len;

            hxhim_type_t object_type;
            void *object;
            std::size_t object_len;
        };

        /** @description Convenience struct for GET2 results    */
        struct Get2 : public Result {
            ~Get2();

            void *subject;
            std::size_t subject_len;

            void *predicate;
            std::size_t predicate_len;

            hxhim_type_t object_type;
            void *object;
            std::size_t *object_len;
        };

        /** @description Convenience struct for DEL results */
        struct Delete : public Result {};

        /** @description Convenience struct for SYNC results */
        struct Sync : public Result {};

        /** @description Convenience struct for HISTOGRAM results */
        struct Histogram : public Result {
            double *buckets;
            std::size_t *counts;
            std::size_t size;
        };

    public:
        Results(hxhim_t *hx);
        ~Results();

        static void Destroy(hxhim_t *hx, Results *res);

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

/**
 * Contructors for hxhim::Results::Result are kept separate
 * in order to not have to include transport/Messages/Messages.hpp
 * They also allocate the objects.
 *
 * Destructors for hxhim::Results::Result are kept separate
 * in order to be symmetric with the constructors and allow
 * for deallocation of the object.
 */
namespace Result {
    void destroy(hxhim_t *hx, Results::Result    *res);
    void destroy(hxhim_t *hx, Results::Put       *put);
    void destroy(hxhim_t *hx, Results::Get       *get);
    void destroy(hxhim_t *hx, Results::Get2      *get);
    void destroy(hxhim_t *hx, Results::Delete    *del);
    void destroy(hxhim_t *hx, Results::Sync      *sync);
    void destroy(hxhim_t *hx, Results::Histogram *hist);
}

}

#endif
