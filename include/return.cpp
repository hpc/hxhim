#include "return.h"
#include "return.hpp"

namespace hxhim {

Return::Return(enum hxhim_work_op operation, bool msg_sent, TransportMessage *message)
  : op(operation),
    sent(msg_sent),
    msg(message),
    next(nullptr)
{}

Return::~Return() {
    delete msg;
}

int Return::GetSrc() const {
    if (!msg) {
        return HXHIM_ERROR;
    }

    return msg->src;
}

hxhim_work_op Return::GetOp() const {
    return op;
}

int Return::GetError() const {
    if (!msg) {
        return HXHIM_ERROR;
    }

    switch (msg->mtype) {
        case TransportMessageType::RECV:
            return dynamic_cast<TransportRecvMessage *>(msg)->error;
        case TransportMessageType::RECV_GET:
            return dynamic_cast<TransportGetRecvMessage *>(msg)->error;
        case TransportMessageType::RECV_BGET:
            return dynamic_cast<TransportBGetRecvMessage *>(msg)->error;
        case TransportMessageType::RECV_BULK:
            return dynamic_cast<TransportBRecvMessage *>(msg)->error;
        default:
            return HXHIM_ERROR;
    }

    return HXHIM_ERROR;
}

Return *Return::Next() const {
    return next;
}

Return *Return::Next(Return *ret) {
    return (next = ret);
}

GetReturn::GetReturn(enum hxhim_work_op operation, TransportGetRecvMessage *grm)
  : Return(operation, true, grm),
    pos(0),
    curr(msg)
{}

GetReturn::GetReturn(enum hxhim_work_op operation, TransportBGetRecvMessage *bgrm)
    : Return(operation, true, bgrm),
      pos(0),
      curr(msg)
{}

void GetReturn::MoveToFirstRS() {
    curr = msg;

    MoveToFirstKV();
}

void GetReturn::NextRS() {
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

int GetReturn::ValidRS() const {
    return curr?HXHIM_SUCCESS:HXHIM_ERROR;
}

void GetReturn::MoveToFirstKV() {
    pos = 0;
}

int GetReturn::PrevKV() {
    return ValidKV(pos -= (bool) pos);
}

int GetReturn::NextKV() {
    const int ret = ValidKV(++pos);
    return ret;
}

int GetReturn::ValidKV() const {
    return ValidKV(pos);
}

int GetReturn::GetKV(void **key, std::size_t *key_len, void **value, std::size_t *value_len) {
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

int GetReturn::ValidKV(const std::size_t position) const {
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

}

void hxhim_return_destroy(hxhim_return_t *ret) {
    if (ret) {
        delete ret->ret;
    }

    delete ret;
}

void hxhim_get_return_destroy(hxhim_get_return_t *ret) {
    // do not delete ret->ret because it is not owned by hxhim_get_return_t
    delete ret;
}

int hxhim_return_get_src(hxhim_return_t *ret, int *src) {
    if (!ret || !ret->ret || !src) {
        return HXHIM_ERROR;
    }

    *src = ret->ret->GetSrc();
    return HXHIM_SUCCESS;
}

int hxhim_return_get_op(hxhim_return_t *ret, enum hxhim_work_op *op) {
    if (!ret || !ret->ret || !op) {
        return HXHIM_ERROR;
    }

    *op = ret->ret->GetOp();
    return HXHIM_SUCCESS;
}

int hxhim_return_get_error(hxhim_return_t *ret, int *error) {
    if (!ret || !ret->ret || !error) {
        return HXHIM_ERROR;
    }

    *error = ret->ret->GetError();
    return HXHIM_SUCCESS;
}

int hxhim_return_convert_to_get(hxhim_return_t *ret, hxhim_get_return_t **get) {
    if (!ret || !ret->ret || !get) {
        return HXHIM_ERROR;
    }

    *get = nullptr;
    if (ret->ret->GetOp() == hxhim_work_op::HXHIM_GET) {
        *get = new hxhim_get_return_t();
        (*get)->ret = dynamic_cast<hxhim::GetReturn *>(ret->ret);
    }

    return HXHIM_SUCCESS;
}

int hxhim_return_next(hxhim_return_t *ret, hxhim_return_t **next) {
    if (!ret || !ret->ret || !next) {
        return HXHIM_ERROR;
    }

    *next = nullptr;
    if (ret->ret->Next()) {
        *next = new hxhim_return_t();
        (*next)->ret = ret->ret->Next();
    }
    return HXHIM_SUCCESS;
}

int hxhim_get_return_move_to_first_rs(hxhim_get_return_t *ret) {
    if (!ret || !ret->ret) {
        return HXHIM_ERROR;
    }

    ret->ret->MoveToFirstRS();
    return HXHIM_SUCCESS;
}

int hxhim_get_return_next_rs(hxhim_get_return_t *ret) {
    if (!ret || !ret->ret) {
        return HXHIM_ERROR;
    }

    ret->ret->NextRS();
    return HXHIM_SUCCESS;
}

int hxhim_get_return_valid_rs(hxhim_get_return_t *ret, int *valid) {
    if (!ret || !ret->ret || !valid) {
        return HXHIM_ERROR;
    }

    *valid = ret->ret->ValidRS();
    return HXHIM_SUCCESS;
}

int hxhim_get_return_move_to_first_kv(hxhim_get_return_t *ret) {
    if (!ret || !ret->ret) {
        return HXHIM_ERROR;
    }

    ret->ret->MoveToFirstKV();
    return HXHIM_SUCCESS;
}

int hxhim_get_return_prev_kv(hxhim_get_return_t *ret) {
    if (!ret || !ret->ret) {
        return HXHIM_ERROR;
    }

    ret->ret->PrevKV();
    return HXHIM_SUCCESS;
}

int hxhim_get_return_next_kv(hxhim_get_return_t *ret) {
    if (!ret || !ret->ret) {
        return HXHIM_ERROR;
    }

    ret->ret->NextKV();
    return HXHIM_SUCCESS;
}

int hxhim_get_return_valid_kv(hxhim_get_return_t *ret, int *valid) {
    if (!ret || !ret->ret || !valid) {
        return HXHIM_ERROR;
    }

    *valid = ret->ret->ValidKV();
    return HXHIM_SUCCESS;
}

int hxhim_get_return_get_kv(hxhim_get_return_t *ret, void **key, size_t *key_len, void **value, size_t *value_len) {
    if (!ret || !ret->ret) {
        return HXHIM_ERROR;
    }

    return ret->ret->GetKV(key, key_len, value, value_len);;
}
