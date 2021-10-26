#ifndef MESSAGE_STRUCTS_HPP
#define MESSAGE_STRUCTS_HPP

#include "message/constants.hpp"
#include "message/Message.hpp"

#include "message/BPut.hpp"
#include "message/BGet.hpp"
#include "message/BGetOp.hpp"
#include "message/BDelete.hpp"
#include "message/BHistogram.hpp"

#include "message/Packer.hpp"
#include "message/Unpacker.hpp"

namespace Message {

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
