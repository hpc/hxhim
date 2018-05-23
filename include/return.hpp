#ifndef HXHIM_RETURN_HPP
#define HXHIM_RETURN_HPP

#include "hxhim-types.h"
#include "hxhim_private.hpp"
#include "hxhim_work_op.h"
#include "transport.hpp"

namespace hxhim {

/**
 * Return
 * This class is a container for storing
 * the reponses of MDHIM operations.
 *
 * Each instance of Return represents the response
 * from a single range server, with a pointer
 * pointing to the next range server (if there
 * is one).
 */
class Return {
    public:
        Return(enum hxhim_work_op operation, TransportResponseMessage *response = nullptr);
        virtual ~Return();

        int GetSrc() const;
        hxhim_work_op GetOp() const;
        int GetError() const;

        // Range Server Operations
        int MoveToFirstRS();
        int NextRS();
        int ValidRS() const;

        // Key Value Pair Operations
        // Only valid when op is HXHIM_GET
        int MoveToFirstKV();
        int PrevKV();
        int NextKV();
        int ValidKV() const;
        int GetKV(void **key, std::size_t *key_len, void **value, std::size_t *value_len);

        Return *Next(Return *ret);
        Return *Next() const;

    private:
        int ValidKV(const std::size_t position) const;

        hxhim_work_op op;               // which operation's response this return structure contains
        TransportResponseMessage *head; // the head of the response list

        TransportResponseMessage *curr; // current range server
        std::size_t pos;                // current key index

        Return *next;
};

}

#ifdef __cplusplus
extern "C"
{
#endif

/** Opaque structure returned by hxhimFlush */
typedef struct hxhim_return {
    hxhim::Return *head;
    hxhim::Return *curr;
} hxhim_return_t;

#ifdef __cplusplus
}
#endif

#endif
