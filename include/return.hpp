#ifndef HXHIM_RETURN_HPP
#define HXHIM_RETURN_HPP

#include "hxhim-types.h"
#include "hxhim_private.hpp"
#include "transport.hpp"

namespace hxhim {

class Return {
    public:
        Return(work_t::Op operation, bool msg_sent, TransportMessage *message = nullptr)
          : op(operation),
            sent(msg_sent),
            msg(message),
            next(nullptr)
        {}

        virtual ~Return() {
            delete msg;
        }

        int GetSrc() const {
            if (!msg) {
                return HXHIM_ERROR;
            }

            return msg->src;
        }

        work_t::Op GetOp() const {
            return op;
        }

        int GetError() const {
            if (!msg) {
                return HXHIM_ERROR;
            }

            switch (msg->mtype) {
                case TransportMessageType::RECV:
                    return dynamic_cast<TransportRecvMessage *>(msg)->error;
                    break;
                case TransportMessageType::RECV_GET:
                    return dynamic_cast<TransportGetRecvMessage *>(msg)->error;
                    break;
                case TransportMessageType::RECV_BGET:
                    return dynamic_cast<TransportBGetRecvMessage *>(msg)->error;
                    break;
                case TransportMessageType::RECV_BULK:
                    return dynamic_cast<TransportBRecvMessage *>(msg)->error;
                    break;
                default:
                    return HXHIM_ERROR;
            }

            return HXHIM_ERROR;
        }

        Return *Next() const {
            return next;
        }

        Return *Next(Return *ret) {
            return (next = ret);
        }

    protected:
        work_t::Op op;
        bool sent;
        TransportMessage *msg;

        Return *next;
};

class GetReturn : public Return {
    public:
        GetReturn(work_t::Op operation, TransportGetRecvMessage *grm)
          : Return(operation, true, grm),
            pos(0),
            curr(msg)
        {}

        GetReturn(work_t::Op operation, TransportBGetRecvMessage *bgrm)
          : Return(operation, true, bgrm),
            pos(0),
            curr(msg)
        {}

        void MoveToFirstRS() {
            curr = msg;

            MoveToFirstKV();
        }

        void NextRS() {
            if (curr) {
                switch (curr->mtype) {
                    case TransportMessageType::RECV_BGET:
                        curr = dynamic_cast<TransportBGetRecvMessage *>(curr)->next;
                        break;
                    case TransportMessageType::RECV_GET:
                    default:
                        curr = nullptr;
                        break;
                }
            }
        }

        int ValidRS() const {
            return curr?HXHIM_SUCCESS:HXHIM_ERROR;
        }

        void MoveToFirstKV() {
            pos = 0;
        }

        int PrevKV() {
            return ValidKV(pos -= (bool) pos);
        }

        int NextKV() {
            const int ret = ValidKV(++pos);
            return ret;
        }

        int ValidKV() const {
            return ValidKV(pos);
        }

        int GetKV(void **key, std::size_t *key_len, void **value, std::size_t *value_len) {
            if (ValidKV() != HXHIM_SUCCESS) {
                return HXHIM_ERROR;
            }

            switch (curr->mtype) {
                case TransportMessageType::RECV_GET:
                    {
                        TransportGetRecvMessage *grm = dynamic_cast<TransportGetRecvMessage *>(curr);
                        if (key) {
                            *key = grm->key;
                        }

                        if (key_len) {
                            *key_len = grm->key_len;
                        }

                        if (value) {
                            *value = grm->value;
                        }

                        if (value_len) {
                            *value_len = grm->value_len;
                        }
                    }
                    break;
                case TransportMessageType::RECV_BGET:
                    {
                        TransportBGetRecvMessage *bgrm = dynamic_cast<TransportBGetRecvMessage *>(curr);
                        if (key) {
                            *key = bgrm->keys[pos];
                        }

                        if (key_len) {
                            *key_len = bgrm->key_lens[pos];
                        }

                        if (value) {
                            *value = bgrm->values[pos];
                        }

                        if (value_len) {
                            *value_len = bgrm->value_lens[pos];
                        }
                    }
                    break;
                default:
                    return HXHIM_ERROR;
            }

            return HXHIM_SUCCESS;
        }

    private:
        int ValidKV(const std::size_t position) const {
            if (!curr) {
                return HXHIM_ERROR;
            }

            int ret = HXHIM_SUCCESS;
            switch (curr->mtype) {
                case TransportMessageType::RECV_GET:
                    if (position > 0) {
                        ret = HXHIM_ERROR;
                    }
                    break;
                case TransportMessageType::RECV_BGET:
                    if (position >= dynamic_cast<TransportBGetRecvMessage *>(curr)->num_keys) {
                        ret = HXHIM_ERROR;
                    }
                    break;
                default:
                    ret = HXHIM_ERROR;
                    break;
            }

            return ret;
        }

        std::size_t pos;
        TransportMessage *curr;
};

}

#endif
