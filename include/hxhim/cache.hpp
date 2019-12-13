#ifndef HXHIM_CACHE_HPP
#define HXHIM_CACHE_HPP

#include <condition_variable>
#include <mutex>
#include <type_traits>

#include "hxhim/constants.h"
#include "transport/Messages/Messages.hpp"
#include "utils/enable_if_t.hpp"

namespace hxhim {
    struct UserData {
        virtual ~UserData();
    };

    typedef struct SubjectPredicate : UserData {
        SubjectPredicate();
        virtual ~SubjectPredicate();

        void *subject;
        std::size_t subject_len;

        void *predicate;
        std::size_t predicate_len;
    } SP_t;

    typedef struct SubjectPredicateObject : SP_t {
        SubjectPredicateObject();
        virtual ~SubjectPredicateObject();

        hxhim_type_t object_type;
        void *object;
        std::size_t object_len;
    } SPO_t;

    struct PutData : SPO_t {
        PutData();
        ~PutData() = default;
        int moveto(Transport::Request::SendBPut *bput, const int ds_offset) const;

        PutData *prev;
        PutData *next;
    };

    struct GetData : SP_t {
        GetData();
        ~GetData();
        int moveto(Transport::Request::SendBGet *bget, const int ds_offset) const;

        hxhim_type_t object_type;
        GetData *prev;
        GetData *next;
    };

    struct GetOpData : SP_t {
        GetOpData();
        ~GetOpData();
        int moveto(Transport::Request::SendBGetOp *bgetop, const int ds_offset) const;

        hxhim_type_t object_type;
        std::size_t num_recs;
        hxhim_get_op_t op;
        GetOpData *prev;
        GetOpData *next;
    };

    struct DeleteData : SP_t {
        DeleteData();
        ~DeleteData() = default;
        int moveto(Transport::Request::SendBDelete *bdelete, const int ds_offset) const;

        DeleteData *prev;
        DeleteData *next;
    };

    // struct BHistogramData : UserData {
    //     BHistogramData();
    //     int moveto(Transport::Request::SendBHistogram *bhist, const int ds_offset) const;
    // };

    template <typename Data, typename = enable_if_t<std::is_base_of<SubjectPredicate, Data>::value> >
    struct Unsent {
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
