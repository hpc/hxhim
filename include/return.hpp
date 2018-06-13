#ifndef HXHIM_RETURN_HPP
#define HXHIM_RETURN_HPP

#include "hxhim-types.h"
#include "hxhim_private.hpp"
#include "hxhim_work_op.h"
#include "transport.hpp"

namespace hxhim {

/**
 * Return
 * Each instance of Return represents the response
 * from a single operation. The response may come
 * from multiple range servers. Return points to one
 * range server at a time, and has functions to
 * iterate through all of the range servers.
 *
 * Multiple Returns chained together represents responses
 * from multiple operations.
 */
class Return {
    public:
        Return(enum hxhim_work_op operatio = HXHIM_NOP, TransportResponseMessage *response = nullptr);
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
        int MoveToFirstSPO();
        int PrevSPO();
        int NextSPO();
        int ValidSPO() const;
        int GetSPO(void **subject, size_t *subject_len, void **predicate, size_t *predicate_len, void **object, size_t *object_len);

        Return *Next(Return *ret);
        Return *Next() const;

    private:
        int ValidSPO(const std::size_t position) const;

        hxhim_work_op op;               // which operation type response this return structure contains
        TransportResponseMessage *head; // the head of the response list

        TransportResponseMessage *curr; // current range server
        std::size_t pos;                // current key index

        Return *next;
};

/** Utility functions */
Return *combine_results(Return *&last, Return *result);
Return *return_results(Return &head);

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
