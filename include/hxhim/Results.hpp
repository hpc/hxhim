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
 *     delete res;
 *
 */
class Results {
    public:
        class Result {
            public:
                Result(hxhim_result_type_t t);
                virtual ~Result();

                hxhim_result_type_t GetType() const;
                int GetDatastore() const;
                int GetStatus() const;

            private:
                const hxhim_result_type_t type;

            protected:
                int datastore;
                int status;
        };

        /** @description Convenience class for PUT results */
        class Put : public Result {
            public:
                Put(hxhim_t *hx, Transport::Response::Put *put);
                Put(hxhim_t *hx, Transport::Response::BPut *bput, const std::size_t i);
                virtual ~Put();
        };

        /** @description Convenience class for GET results */
        class Get : public Result {
            public:
                Get(hxhim_t *hx, Transport::Response::Get *get);
                Get(hxhim_t *hx, Transport::Response::BGet *bget, const std::size_t i);
                Get(hxhim_t *hx, Transport::Response::BGetOp *bgetop, const std::size_t i);
                virtual ~Get();

                hxhim_type_t GetObjectType() const;

                /** Users should not deallocate the pointers returned by these functions */
                int GetSubject(void **subject, std::size_t *subject_len) const;
                int GetPredicate(void **predicate, std::size_t *predicate_len) const;
                int GetObject(void **object, std::size_t *object_len) const;

            protected:
                Get();

                void *sub;
                std::size_t sub_len;

                void *pred;
                std::size_t pred_len;

                hxhim_type_t obj_type;
                void *obj;
                std::size_t obj_len;
        };

        /** @description Convenience class for DEL results */
        class Delete : public Result {
            public:
                Delete(hxhim_t *hx, Transport::Response::Delete *del);
                Delete(hxhim_t *hx, Transport::Response::BDelete *bdel, const std::size_t i);
                virtual ~Delete();
        };

        /** @description Convenience class for SYNC results */
        class Sync : public Result {
            public:
                Sync(hxhim_t *hx, const int ds_offset, const int synced);
                ~Sync();
        };

        /** @description Convenience class for HISTOGRAM results */
        class Histogram : public Result {
            public:
                Histogram(hxhim_t *hx, Transport::Response::Histogram *hist);
                Histogram(hxhim_t *hx, Transport::Response::BHistogram *bhist, const std::size_t i);
                ~Histogram();

                int GetBuckets(double **b) const;
                int GetCounts(std::size_t **c) const;
                int GetSize(std::size_t *s) const;

            private:
                double *buckets;
                std::size_t *counts;
                std::size_t size;
        };

    public:
        Results(hxhim_t *hx);
        ~Results();

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
        std::list<Result *>::iterator curr;
};

}

void hxhim_results_destroy(hxhim_t *hx, hxhim::Results *res);

#endif
