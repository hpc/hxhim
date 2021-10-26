#include "message/Messages.hpp"

::Message::Response::Response *::Message::next(::Message::Response::Response *curr) {
    // curr != nullptr has already been checked
    ::Message::Response::Response *next = curr->next;
    destruct(curr);
    return next;
}
