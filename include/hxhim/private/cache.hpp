#ifndef HXHIM_CACHE_HPP
#define HXHIM_CACHE_HPP

#include <condition_variable>
#include <list>
#include <mutex>

#include "hxhim/constants.h"
#include "hxhim/Stats.hpp"
#include "transport/Messages/Messages.hpp"
#include "utils/Blob.hpp"
#include "utils/Stats.hpp"
#include "utils/type_traits.hpp"

/*
 * These structs do not own any of the data they hold.
 * They are all references, and will not be cleaned up
 * by the destructor.
 *
 * These structs are meant to be used as nodes of an
 * Unsent which are destructed when they areremoved.
 * Unsent does not destruct the nodes.
 */
namespace hxhim {
    struct UserData {
        UserData();
        virtual ~UserData();

        int ds_id;
        int ds_rank;
        int ds_offset;

        struct hxhim::Stats::Send timestamps;
    };

    typedef struct SubjectPredicate : UserData {
        SubjectPredicate();
        virtual ~SubjectPredicate();

        Blob *subject;
        Blob *predicate;
    } SP_t;

    struct PutData : SP_t {
        PutData();
        ~PutData() = default;
        int moveto(Transport::Request::BPut *bput);

        hxhim_object_type_t object_type;
        Blob *object;

        PutData *prev;
        PutData *next;
    };

    struct GetData : SP_t {
        GetData();
        ~GetData();
        int moveto(Transport::Request::BGet *bget);

        hxhim_object_type_t object_type;

        GetData *prev;
        GetData *next;
    };

    struct GetOpData : SP_t {
        GetOpData();
        ~GetOpData();
        int moveto(Transport::Request::BGetOp *bgetop);

        hxhim_object_type_t object_type;
        std::size_t num_recs;
        hxhim_get_op_t op;

        GetOpData *prev;
        GetOpData *next;
    };

    struct DeleteData : SP_t {
        DeleteData();
        ~DeleteData() = default;
        int moveto(Transport::Request::BDelete *bdelete);

        DeleteData *prev;
        DeleteData *next;
    };

    template <typename Data, typename = enable_if_t<std::is_base_of<SubjectPredicate, Data>::value> >
    struct Unsent {
        Unsent()
            : mutex(),
              start_processing(),
              done_processing(),
              head(nullptr),
              tail(nullptr),
              count(0),
              force(false)
        {}

        void insert(Data *node) {
            if (!node) {
                return;
            }

            std::lock_guard<std::mutex> lock(mutex);

            if (head) {
                tail->next = node;
                node->prev = tail;
            }
            else {
                head = node;
                node->prev = nullptr;
            }

            tail = node;
            node->next = nullptr;
            count++;
        }

        std::mutex mutex;
        std::condition_variable start_processing;
        std::condition_variable done_processing;
        Data *head;
        Data *tail;
        std::size_t count;
        bool force;
    };
}

#endif
