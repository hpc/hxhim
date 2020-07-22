#include "transport/Messages/Messages.hpp"

Transport::Response::Response *Transport::next(Transport::Response::Response *curr) {
    if (!curr) {
        return nullptr;
    }

    Transport::Response::Response *next = nullptr;
    switch (curr->op) {
        case hxhim_op_t::HXHIM_PUT:
            next = static_cast<Transport::Response::BPut *>(curr)->next;
            break;
        case hxhim_op_t::HXHIM_GET:
            next = static_cast<Transport::Response::BGet *>(curr)->next;
            break;
        case hxhim_op_t::HXHIM_GETOP:
            next = static_cast<Transport::Response::BGetOp *>(curr)->next;
            break;
        case hxhim_op_t::HXHIM_DELETE:
            next = static_cast<Transport::Response::BDelete *>(curr)->next;
            break;
        default:
            break;
    }

    destruct(curr);
    return next;
}
