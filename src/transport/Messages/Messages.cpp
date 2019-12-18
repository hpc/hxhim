#include "transport/Messages/Messages.hpp"

Transport::Response::Response *Transport::next(Transport::Response::Response *curr) {
    if (!curr) {
        return nullptr;
    }

    Transport::Response::Response *next = nullptr;
    switch (curr->type) {
        case Transport::Message::BPUT:
            next = static_cast<Transport::Response::BPut *>(curr)->next;
            break;
        case Transport::Message::BGET2:
            next = static_cast<Transport::Response::BGet2 *>(curr)->next;
            break;
        case Transport::Message::BGETOP:
            next = static_cast<Transport::Response::BGetOp *>(curr)->next;
            break;
        case Transport::Message::BDELETE:
            next = static_cast<Transport::Response::BDelete *>(curr)->next;
            break;
        default:
            break;
    }

    destruct(curr);
    return next;
}
