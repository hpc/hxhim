#ifndef HXHIM_MESSAGE_STRUCTS_HPP
#define HXHIM_MESSAGE_STRUCTS_HPP

#include "transport/Messages/BPut.hpp"
#include "transport/Messages/BGet.hpp"
#include "transport/Messages/BGetOp.hpp"
#include "transport/Messages/BDelete.hpp"
#include "transport/Messages/BHistogram.hpp"
#include "transport/Messages/Packer.hpp"
#include "transport/Messages/Unpacker.hpp"
#include "utils/type_traits.hpp"

namespace Transport {

/**
 * next
 * Goes to the next message of a message chain
 * Deletes the current message
 *
 * @tparam curr A pointer to a bulk message
 * @return the next message in the chain
 */
Response::Response *next(Response::Response *curr);
}

#endif
