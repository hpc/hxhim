#include "return.h"
#include "return.hpp"

namespace hxhim {

Return::Return(enum hxhim_work_op operation, TransportResponseMessage *response)
  : op(operation),
    head(response),
    curr(head),
    pos(0),
    next(nullptr)
{}

/**
 * ~Return
 * The destructor of Return deletes itself
 * as well as all linked Returns
 */
Return::~Return() {
    delete head;
    delete next;
}

/**
 * GetSrc
 * Gets the range server this response came from
 *
 * @return the range server or MDHIM_ERROR
 */
int Return::GetSrc() const {
    if (!curr) {
        return HXHIM_ERROR;
    }

    return curr->src;
}

/**
 * GetOp
 *
 * @return the hxhim_work_op this Return contains
 */
hxhim_work_op Return::GetOp() const {
    return op;
}

/**
 * GetError
 *
 * @return the error message of the response, or HXHIM_ERROR
 */
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

/**
 * MoveToFirstRS
 * Moves the internal pointer to the first range server.
 * This function should be the first function called when
 * iterating through the responses.
 *
 * @return HXHIM_SUCCESS if the current pointer is valid, otherwise HXHIM_ERROR
 */
int Return::MoveToFirstRS() {
    curr = head;

    return MoveToFirstKV();
}

/**
 * NextRS
 * Moves the internal pointer to the next range server's responses
 *
 * @return HXHIM_SUCCESS if the current pointer is valid, otherwise HXHIM_ERROR
 */
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

    return MoveToFirstKV();
}

/**
 * ValidRS
 *
 * @return HXHIM_SUCCESS if the current pointer is valid, otherwise HXHIM_ERROR
 */
int Return::ValidRS() const {
    return curr?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * MoveToFirstKV
 * Moves the KV index to the first KV pair
 *
 * @return HXHIM_SUCCESS if the current pointer is valid, otherwise HXHIM_ERROR
 */
int Return::MoveToFirstKV() {
    pos = 0;
    return curr?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * PrevKV
 * Moves the KV index to the previous index.
 *
 * @return HXHIM_SUCCESS if the index is valid, otherwise HXHIM_ERROR
 */
int Return::PrevKV() {
    return ValidKV(--pos);
}

/**
 * NextKV
 * Moves the KV index to the next index
 *
 * @return HXHIM_SUCCESS if the index is valid, otherwise HXHIM_ERROR
 */
int Return::NextKV() {
    return ValidKV(++pos);
}

/**
 * ValidKV
 *
 * @return HXHIM_SUCCESS if the index is valid, otherwise HXHIM_ERROR
 */
int Return::ValidKV() const {
    return ValidKV(pos);
}

/**
 * GetKV
 * Fills in the key and value data at the current KV index, if it is valid.
 * All of the parameters are optional
 *
 * @param key       address of a pointer that will be set to the address of the key
 * @param key_len   address of a size_t that will be set to the length of the key
 * @param value     address of a pointer that will be set to the address of the value
 * @param value_len address of a size_t that will be set to the length of the value
 *
 * @return HXHIM_SUCCESS if the index is valid, otherwise HXHIM_ERROR
 */
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

/**
 * ValidKV
 * Whether or not the given KV index is valid
 *
 * @return HXHIM_SUCCESS if the index is valid, otherwise HXHIM_ERROR
 */
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

/**
 * Next
 * Set the next Return value
 *
 * @param ret the pointer to set Return::next to
 * @return ret if ret is not NULL. Otherwise returns this. This is done to make sure the return value of Return::Next always exists
 */
Return *Return::Next(Return *ret) {
    if (!(next = ret)) {
        return this;
    }
    return next;
}

/**
 * Next
 *
 * @return the current Return value following *this
 */
Return *Return::Next() const {
    return next;
}

}

/**
 * hxhim_return_destroy
 * Cleans up all of the response from a Flush
 *
 * @param ret the HXHIM instance
 */
void hxhim_return_destroy(hxhim_return_t *ret) {
    if (ret) {
        delete ret;
    }
}

/**
 * hxhim_return_get_src
 *
 * @param ret the response
 * @param src the current response's range server rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_get_src(hxhim_return_t *ret, int *src) {
    if (!ret || !ret->curr || !src) {
        return HXHIM_ERROR;
    }

    *src = ret->curr->GetSrc();
    return HXHIM_SUCCESS;
}

/**
 * hxhim_return_get_op
 *
 * @param ret the response
 * @param op  the operation that this
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_get_op(hxhim_return_t *ret, enum hxhim_work_op *op) {
    if (!ret || !ret->curr || !op) {
        return HXHIM_ERROR;
    }

    *op = ret->curr->GetOp();
    return HXHIM_SUCCESS;
}

/**
 * hxhim_return_get_error
 *
 * @param ret   the response
 * @param error the status returned from the current range server
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_get_error(hxhim_return_t *ret, int *error) {
    if (!ret || !ret->curr || !error) {
        return HXHIM_ERROR;
    }

    *error = ret->curr->GetError();
    return HXHIM_SUCCESS;
}

/**
 * hxhim_return_move_to_first_rs
 * Moves the internal pointer to the first range server.
 * This function should be the first function called when
 * iterating through the responses.
 *
 * @param ret the response
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_move_to_first_rs(hxhim_return_t *ret) {
    if (!ret || !ret->curr) {
        return HXHIM_ERROR;
    }

    return ret->curr->MoveToFirstRS();
}

/**
 * hxhim_return_next_rs
 * Moves the internal pointer to the next range server's responses
 *
 * @param ret the response
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_next_rs(hxhim_return_t *ret) {
    if (!ret || !ret->curr) {
        return HXHIM_ERROR;
    }

    return ret->curr->NextRS();
}

/**
 * hxhim_return_valid_rs
 *
 * @param ret   the response
 * @param valid whether or not the current range server is valid
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_valid_rs(hxhim_return_t *ret, int *valid) {
    if (!ret || !ret->curr || !valid) {
        return HXHIM_ERROR;
    }

    *valid = ret->curr->ValidRS();
    return HXHIM_SUCCESS;
}

/**
 * hxhim_return_move_to_first_kv
 * Moves the internal KV index to 0
 *
 * @param ret the response
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_move_to_first_kv(hxhim_return_t *ret) {
    if (!ret || !ret->curr) {
        return HXHIM_ERROR;
    }

    return ret->curr->MoveToFirstKV();
}

/**
 * hxhim_return_prev_kv
 * Moves the internal KV index to the previous index.
 *
 * @param ret the response
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_prev_kv(hxhim_return_t *ret) {
    if (!ret || !ret->curr) {
        return HXHIM_ERROR;
    }

    return ret->curr->PrevKV();
}

/**
 * hxhim_return_next_kv
 * Moves the KV index to the next index
 *
 * @param ret the response
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_next_kv(hxhim_return_t *ret) {
    if (!ret || !ret->curr) {
        return HXHIM_ERROR;
    }

    return ret->curr->NextKV();
}

/**
 * hxhim_return_valid_kv
 *
 * @param ret the response
 * @param valid whether or not the current KV pair (index) is valid
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_valid_kv(hxhim_return_t *ret, int *valid) {
    if (!ret || !ret->curr || !valid) {
        return HXHIM_ERROR;
    }

    *valid = ret->curr->ValidKV();
    return HXHIM_SUCCESS;
}

/**
 * hxhim_return_get_kv
 *
 * @param ret       the response
 * @param key       address of a pointer that will be set to the address of the key
 * @param key_len   address of a size_t that will be set to the length of the key
 * @param value     address of a pointer that will be set to the address of the value
 * @param value_len address of a size_t that will be set to the length of the value
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_get_kv(hxhim_return_t *ret, void **key, size_t *key_len, void **value, size_t *value_len) {
    if (!ret || !ret->curr) {
        return HXHIM_ERROR;
    }

    return ret->curr->GetKV(key, key_len, value, value_len);;
}

/**
 * hxhim_return_next
 * Moves to the next range server's response
 *
 * @param ret the response
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_next(hxhim_return_t *ret) {
    if (!ret || !ret->curr) {
        return HXHIM_ERROR;
    }

    ret->curr = ret->curr->Next();
    return ret->curr?HXHIM_SUCCESS:HXHIM_ERROR;
}
