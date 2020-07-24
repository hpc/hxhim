#include "transport/Messages/Messages.hpp"

Transport::Response::Response *Transport::next(Transport::Response::Response *curr) {
    // curr != nullptr has already been checked
    Transport::Response::Response *next = curr->next;
    destruct(curr);
    return next;
}
