#include "return.h"
#include "return.hpp"
#include <iostream>

namespace hxhim {

Return::Return(enum hxhim_work_op operation, TransportResponseMessage *response)
  : op(operation),
    head(response),
    curr(head),
    pos(0)
{}

Return::~Return() {
    delete head;
}

int Return::GetSrc() const {
    if (!curr) {
        return HXHIM_ERROR;
    }

    return curr->src;
}

hxhim_work_op Return::GetOp() const {
    return op;
}

int Return::GetError() const {
    if (!curr) {
        return HXHIM_ERROR;
    }

    switch (curr->mtype) {
        case TransportMessageType::RECV:
            return dynamic_cast<TransportRecvMessage *>(curr)->error;
        case TransportMessageType::RECV_GET:
            return dynamic_cast<TransportGetRecvMessage *>(curr)->error;
        case TransportMessageType::RECV_BGET:
            return dynamic_cast<TransportBGetRecvMessage *>(curr)->error;
        case TransportMessageType::RECV_BULK:
            return dynamic_cast<TransportBRecvMessage *>(curr)->error;
        default:
            return HXHIM_ERROR;
    }

    return HXHIM_ERROR;
}

int Return::MoveToFirstRS() {
    curr = head;

    return MoveToFirstKV();
}

int Return::NextRS() {
    if (!curr) {
        return HXHIM_ERROR;
    }

    switch (curr->mtype) {
        case TransportMessageType::RECV_BULK:
            curr = dynamic_cast<TransportBRecvMessage *>(curr)->next;
            break;
        case TransportMessageType::RECV_BGET:
            curr = dynamic_cast<TransportBGetRecvMessage *>(curr)->next;
            break;
        default:
            curr = nullptr;
            break;
    }

    return curr?HXHIM_SUCCESS:HXHIM_ERROR;
}

int Return::ValidRS() const {
    return curr?HXHIM_SUCCESS:HXHIM_ERROR;
}

int Return::MoveToFirstKV() {
    pos = 0;
    return curr?HXHIM_SUCCESS:HXHIM_ERROR;
}

int Return::PrevKV() {
    return ValidKV(pos -= (bool) pos);
}

int Return::NextKV() {
    return ValidKV(++pos);
}

int Return::ValidKV() const {
    return ValidKV(pos);
}

int Return::GetKV(void **key, std::size_t *key_len, void **value, std::size_t *value_len) {
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

int Return::ValidKV(const std::size_t position) const {
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

int hxhim_return_move_to_first_rs(hxhim_return_t *ret) {
    if (!ret || !ret->ret) {
        return HXHIM_ERROR;
    }

    return ret->ret->MoveToFirstRS();
}

int hxhim_return_next_rs(hxhim_return_t *ret) {
    if (!ret || !ret->ret) {
        return HXHIM_ERROR;
    }

    return ret->ret->NextRS();
}

int hxhim_return_valid_rs(hxhim_return_t *ret, int *valid) {
    if (!ret || !ret->ret || !valid) {
        return HXHIM_ERROR;
    }

    *valid = ret->ret->ValidRS();
    return HXHIM_SUCCESS;
}

int hxhim_return_move_to_first_kv(hxhim_return_t *ret) {
    if (!ret || !ret->ret) {
        return HXHIM_ERROR;
    }

    return ret->ret->MoveToFirstKV();
}

int hxhim_return_prev_kv(hxhim_return_t *ret) {
    if (!ret || !ret->ret) {
        return HXHIM_ERROR;
    }

    return ret->ret->PrevKV();
}

int hxhim_return_next_kv(hxhim_return_t *ret) {
    if (!ret || !ret->ret) {
        return HXHIM_ERROR;
    }

    return ret->ret->NextKV();
}

int hxhim_return_valid_kv(hxhim_return_t *ret, int *valid) {
    if (!ret || !ret->ret || !valid) {
        return HXHIM_ERROR;
    }

    *valid = ret->ret->ValidKV();
    return HXHIM_SUCCESS;
}

int hxhim_return_get_kv(hxhim_return_t *ret, void **key, size_t *key_len, void **value, size_t *value_len) {
    if (!ret || !ret->ret) {
        return HXHIM_ERROR;
    }

    return ret->ret->GetKV(key, key_len, value, value_len);;
}
