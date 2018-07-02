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
 */
class Results {
    public:
        // Structs for storing results
        // These structs only hold data, which should to be
        // accessed directly instead of through accessors.
        struct Result {
            Result(hxhim_result_type t = hxhim_result_type::HXHIM_RESULT_NONE, int err = HXHIM_SUCCESS, int db = -1);
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
        Results();
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

        Result *head;
        Result *tail;
        Result *curr;
};

}

#endif
