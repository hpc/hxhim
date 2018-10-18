#ifndef HXHIM_MESSAGE_STRUCTS_HPP
#define HXHIM_MESSAGE_STRUCTS_HPP

#include <type_traits>

#include "transport/Messages/Put.hpp"
#include "transport/Messages/Get.hpp"
#include "transport/Messages/Delete.hpp"
#include "transport/Messages/BPut.hpp"
#include "transport/Messages/BGet.hpp"
#include "transport/Messages/BGetOp.hpp"
#include "transport/Messages/BDelete.hpp"
#include "transport/Messages/Histogram.hpp"
#include "transport/Messages/BHistogram.hpp"
#include "utils/FixedBufferPool.hpp"
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
Msg *next(Msg *curr, FixedBufferPool *fbp) {
    if (!curr) {
        return nullptr;
    }

    Msg *next = curr->next;
    fbp->release(curr);
    return next;
}

}

#endif
