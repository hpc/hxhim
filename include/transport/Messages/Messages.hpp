#ifndef HXHIM_MESSAGE_STRUCTS_HPP
#define HXHIM_MESSAGE_STRUCTS_HPP

#include <type_traits>

#include "transport/Messages/SendBPut.hpp"
#include "transport/Messages/SendBGet.hpp"
// #include "transport/Messages/SendBGet2.hpp"
#include "transport/Messages/SendBGetOp.hpp"
#include "transport/Messages/SendBDelete.hpp"
// #include "transport/Messages/SendBHistogram.hpp"

#include "transport/Messages/RecvBPut.hpp"
#include "transport/Messages/RecvBGet.hpp"
// #include "transport/Messages/RecvBGet2.hpp"
#include "transport/Messages/RecvBGetOp.hpp"
#include "transport/Messages/RecvBDelete.hpp"
// #include "transport/Messages/RecvBHistogram.hpp"

#include "transport/Messages/Packer.hpp"
#include "transport/Messages/Unpacker.hpp"
#include "utils/enable_if_t.hpp"

namespace Transport {

/**
 * next
 * Goes to the next message of a bulk message chain
 * Deletes the current message
 *
 * @tparam curr A pointer to a bulk message
 * @param  fbp  The FixedBufferPool the message should be deallocated from
 * @return the next message in the chain
 */
template <typename Msg, typename = enable_if_t <std::is_base_of<Transport::Bulk,    Msg>::value &&
                                                std::is_base_of<Transport::Message, Msg>::value> >
Msg *next(Msg *curr) {
    if (!curr) {
        return nullptr;
    }

    Msg *next = curr->next;
    dealloc(curr);
    return next;
}

}

#endif
