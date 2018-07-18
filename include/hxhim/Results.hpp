#ifndef HXHIM_RESULTS_HPP
#define HXHIM_RESULTS_HPP

#include <cstddef>

#include "constants.h"
#include "Results.h"

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
        // Structs for storing results
        // These structs only hold data, which should to be
        // accessed directly instead of through accessors.
        struct Result {
            Result(hxhim_result_type_t t = HXHIM_RESULT_NONE, int err = HXHIM_SUCCESS, int db = -1);
            virtual ~Result();

            const hxhim_result_type type;
            const int error;
            const int database;

            Result *next;
        };

        struct Put : Result {
            Put(const int err, const int db);
            ~Put();
        };

        struct Get : Result {
            Get(const int err, const int db,
                void *sub, std::size_t sub_len,
                void *pred, std::size_t pred_len,
                void *obj, std::size_t obj_len);
            ~Get();

            void *subject;
            std::size_t subject_len;
            void *predicate;
            std::size_t predicate_len;
            void *object;
            std::size_t object_len;
        };

        struct Delete : Result {
            Delete(const int err, const int db);
            ~Delete();
        };

    public:
        Results(hxhim_spo_type_t sub_type, hxhim_spo_type_t pred_type, hxhim_spo_type_t obj_type);
        ~Results();

        // Accessors (controls the "curr" pointer)
        bool Valid() const;
        Result *GoToHead();
        Result *GoToNext();
        Result *Curr() const;
        Result *Next() const;

        // Modifiers (appends new nodes to the list)
        Result *AddPut(const int error, const int database);
        Result *AddGet(const int error, const int database,
                       void *subject, std::size_t subject_len,
                       void *predicate, std::size_t predicate_len,
                       void *object, std::size_t object_len);
        Result *AddDelete(const int error, const int database);

        // Append another set of results; the list being appended is emptied out
        Results &Append(Results *results);

    private:
        Result *append(Result *single);

        hxhim_spo_type_t subject_type;
        hxhim_spo_type_t predicate_type;
        hxhim_spo_type_t object_type;

        Result *head;
        Result *tail;
        Result *curr;
};

}

#endif
