#ifndef HXHIM_RESULTS_HPP
#define HXHIM_RESULTS_HPP

#include <cstddef>

#include "hxhim/Results.h"
#include "hxhim/constants.h"

namespace hxhim {

/**
 * Results
 * This structure holds a linked list of all results waiting to be processed.
 * A single result node contains exactly 1 set of data. Results of bulk
 * operations are flattened when they are stored.
 *
 * Each result node owns the pointers it holds.
 *
 * Usage:
 *
 *     Results *res = Flush(hx);
 *     for(res->GoToHead(); res->Valid(); res->GoToNext()) {
 *         switch (res->Curr()->type) {
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
        /** @description Base class for storing individual results */
        class Result {
            public:
                Result(hxhim_result_type_t t = HXHIM_RESULT_NONE, int err = HXHIM_SUCCESS, int db = -1);
                virtual ~Result();

                hxhim_result_type_t GetType() const;
                int GetError() const;
                int GetDatabase() const;

                Result *Next() const;
                Result *&Next();

            private:
                const hxhim_result_type_t type;
                const int error;
                const int database;

                Result *next;
        };

        /** @description Convenience class for PUT results */
        class Put : public Result {
            public:
                Put(const int err, const int db);
                virtual ~Put();
        };

        /** @description Convenience class for GET results */
        class Get : public Result {
            public:
                Get(const int err, const int db, hxhim_spo_type_t object_type);
                virtual ~Get();

                hxhim_spo_type_t GetObjectType() const;

                /** Users should not deallocate the pointers returned by these functions */
                int GetSubject(void **subject, std::size_t *subject_len);
                int GetPredicate(void **predicate, std::size_t *predicate_len);
                int GetObject(void **object, std::size_t *object_len);

            protected:
                int decode(const hxhim_spo_type_t type, void *src, const std::size_t &src_len, void **dst, std::size_t *dst_len);

                /** @description These functions should be used to fill in the member variables so that they can be returned by the Get* functions */
                virtual int FillSubject() = 0;
                virtual int FillPredicate() = 0;
                virtual int FillObject() = 0;

                /* @description These variables are used to hold decoded data, so that they only have to be decoded once */
                void *sub;
                std::size_t sub_len;

                void *pred;
                std::size_t pred_len;

                hxhim_spo_type_t obj_type;
                void *obj;
                std::size_t obj_len;
        };

        /** @description Convenience class for DEL results */
        struct Delete : Result {
            Delete(const int err, const int db);
            virtual ~Delete();
        };

    public:
        Results();
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
        Results &Append(Results *results);

    private:
        Result *head;
        Result *tail;
        Result *curr;
};

}

#endif
