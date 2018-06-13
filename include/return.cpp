#include "return.h"
#include "return.hpp"
#include "triplestore.hpp"

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
    delete next;
    delete head;
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
    return MoveToFirstSPO();
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

    return MoveToFirstSPO();
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
 * MoveToFirstSPO
 * Moves the SPO index to the first SPO pair
 *
 * @return HXHIM_SUCCESS if the current pointer is valid, otherwise HXHIM_ERROR
 */
int Return::MoveToFirstSPO() {
    pos = 0;
    return curr?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * PrevSPO
 * Moves the SPO index to the previous index.
 *
 * @return HXHIM_SUCCESS if the index is valid, otherwise HXHIM_ERROR
 */
int Return::PrevSPO() {
    return ValidSPO(--pos);
}

/**
 * NextSPO
 * Moves the SPO index to the next index
 *
 * @return HXHIM_SUCCESS if the index is valid, otherwise HXHIM_ERROR
 */
int Return::NextSPO() {
    return ValidSPO(++pos);
}

/**
 * ValidSPO
 *
 * @return HXHIM_SUCCESS if the index is valid, otherwise HXHIM_ERROR
 */
int Return::ValidSPO() const {
    return ValidSPO(pos);
}

/**
 * GetSPO
 * Fills in the key and value data at the current SPO index, if it is valid.
 * All of the parameters are optional
 *
 * @param subject       address of the pointer to the subject
 * @param subject_len   address of the the subject's length
 * @param predicate     address of the pointer to the predicate
 * @param predicate_len address of the the predicate's length
 * @param object        address of the pointer to the object
 * @param object_len    address of the the object's length
 * @return HXHIM_SUCCESS if the index is valid, otherwise HXHIM_ERROR
 */
int Return::GetSPO(void **subject, size_t *subject_len, void **predicate, size_t *predicate_len, void **object, size_t *object_len) {
    if (ValidSPO() != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    int ret = HXHIM_ERROR;
    switch (curr->mtype) {
        case TransportMessageType::RECV_GET:
            {
                TransportGetRecvMessage *grm = dynamic_cast<TransportGetRecvMessage *>(curr);
                if (grm) {
                    key_to_sp(grm->key, grm->key_len, subject, subject_len, predicate, predicate_len);

                    if (object) {
                        *object = grm->value;
                    }

                    if (object_len) {
                        *object_len = grm->value_len;
                    }

                    ret = HXHIM_SUCCESS;
                }
            }
            break;
        case TransportMessageType::RECV_BGET:
            {
                TransportBGetRecvMessage *bgrm = dynamic_cast<TransportBGetRecvMessage *>(curr);

                if (bgrm) {
                    key_to_sp(bgrm->keys[pos], bgrm->key_lens[pos], subject, subject_len, predicate, predicate_len);

                    if (object) {
                        *object = bgrm->values[pos];
                    }

                    if (object_len) {
                        *object_len = bgrm->value_lens[pos];
                    }

                    ret = HXHIM_SUCCESS;
                }
            }
            break;
        default:
            break;
    }

    return HXHIM_SUCCESS;
}

/**
 * ValidSPO
 * Whether or not the given SPO index is valid
 *
 * @return HXHIM_SUCCESS if the index is valid, otherwise HXHIM_ERROR
 */
int Return::ValidSPO(const std::size_t position) const {
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

/**
 * combine_results
 * Appends result to the last result and returns the new last result
 *
 * @param last   the final result from a previous operation
 * @param result the set of results to append
 * @return       the final result of results
 */
Return *combine_results(Return *&last, Return *result) {
    if (last) {
        for(last = last->Next(result); last->Next(); last = last->Next());
    }
    return last;
}

/**
 * return_results
 *
 * @param head the head of the list, whose only purpose is to store Return::next
 * @return head->Next()
 */
Return *return_results(Return &head) {
    Return *ret = head.Next();
    head.Next(nullptr);
    return ret;
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
        delete ret->head;
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
 * hxhim_return_move_to_first_spo
 * Moves the internal SPO index to 0
 *
 * @param ret the response
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_move_to_first_spo(hxhim_return_t *ret) {
    if (!ret || !ret->curr) {
        return HXHIM_ERROR;
    }

    return ret->curr->MoveToFirstSPO();
}

/**
 * hxhim_return_prev_spo
 * Moves the internal SPO index to the previous index.
 *
 * @param ret the response
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_prev_spo(hxhim_return_t *ret) {
    if (!ret || !ret->curr) {
        return HXHIM_ERROR;
    }

    return ret->curr->PrevSPO();
}

/**
 * hxhim_return_next_spo
 * Moves the SPO index to the next index
 *
 * @param ret the response
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_next_spo(hxhim_return_t *ret) {
    if (!ret || !ret->curr) {
        return HXHIM_ERROR;
    }

    return ret->curr->NextSPO();
}

/**
 * hxhim_return_valid_spo
 *
 * @param ret the response
 * @param valid whether or not the current SPO pair (index) is valid
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_valid_spo(hxhim_return_t *ret, int *valid) {
    if (!ret || !ret->curr || !valid) {
        return HXHIM_ERROR;
    }

    *valid = ret->curr->ValidSPO();
    return HXHIM_SUCCESS;
}

/**
 * hxhim_return_get_spo
 *
 * @param ret           the response
 * @param subject       address of the pointer to the subject
 * @param subject_len   address of the the subject's length
 * @param predicate     address of the pointer to the predicate
 * @param predicate_len address of the the predicate's length
 * @param object        address of the pointer to the object
 * @param object_len    address of the the object's length
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_return_get_spo(hxhim_return_t *ret, void **subject, size_t *subject_len, void **predicate, size_t *predicate_len, void **object, size_t *object_len) {
    if (!ret || !ret->curr) {
        return HXHIM_ERROR;
    }

    return ret->curr->GetSPO(subject, subject_len, predicate, predicate_len, object, object_len);
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
