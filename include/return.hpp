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
 */
class Return {
    public:
        Return(enum hxhim_work_op operation, bool msg_sent, TransportMessage *message = nullptr);
        virtual ~Return();

        int GetSrc() const;
        hxhim_work_op GetOp() const;
        int GetError() const;

        Return *Next() const;
        Return *Next(Return *ret);

    protected:
        hxhim_work_op op;
        bool sent;
        TransportMessage *msg;

        Return *next;
};

/**
 * GetReturn
 * This class is a container for storing
 * the respopnse of a MDHIM GET or BGET operation.
 */
class GetReturn : virtual public Return {
    public:
        explicit GetReturn(enum hxhim_work_op operation, TransportGetRecvMessage *grm);
        explicit GetReturn(enum hxhim_work_op operation, TransportBGetRecvMessage *bgrm);

        // Range Server Operations
        void MoveToFirstRS();
        void NextRS();
        int ValidRS() const;

        // Key Value Pair Operations
        void MoveToFirstKV();
        int PrevKV();
        int NextKV();
        int ValidKV() const;
        int GetKV(void **key, std::size_t *key_len, void **value, std::size_t *value_len);

    private:
        int ValidKV(const std::size_t position) const;

        std::size_t pos;
        TransportMessage *curr;
};

}

#ifdef __cplusplus
extern "C"
{
#endif

/** Opaque structure returned by hxhimFlush */
typedef struct hxhim_return {
    hxhim::Return *ret;
} hxhim_return_t;

/** Opaque structure found within hxhim_return_t */
typedef struct hxhim_get_return {
    hxhim::GetReturn *ret;
} hxhim_get_return_t;

#ifdef __cplusplus
}
#endif

#endif
