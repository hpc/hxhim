#ifndef MESSAGE_UNPACKER_HPP
#define MESSAGE_UNPACKER_HPP

#include "message/Messages.hpp"

namespace Message {

/**
 * Unpacker
 * A collection of functions that unpack
 * formatted buffers into Messages
 *
 * @param message address of the pointer that will be created and unpacked into
 * @param buf     the data to convert into the message
*/
class Unpacker {
    public:
        static int unpack(Request::Request     **req,    void *buf, const std::size_t bufsize);
        static int unpack(Request::BPut        **bpm,    void *buf, const std::size_t bufsize);
        static int unpack(Request::BGet        **bgm,    void *buf, const std::size_t bufsize);
        static int unpack(Request::BGetOp      **bgm,    void *buf, const std::size_t bufsize);
        static int unpack(Request::BDelete     **bdm,    void *buf, const std::size_t bufsize);
        static int unpack(Request::BHistogram  **bhm,    void *buf, const std::size_t bufsize);

        static int unpack(Response::Response   **res,    void *buf, const std::size_t bufsize);
        static int unpack(Response::BPut       **bpm,    void *buf, const std::size_t bufsize);
        static int unpack(Response::BGet       **bgm,    void *buf, const std::size_t bufsize);
        static int unpack(Response::BGetOp     **bgm,    void *buf, const std::size_t bufsize);
        static int unpack(Response::BDelete    **bdm,    void *buf, const std::size_t bufsize);
        static int unpack(Response::BHistogram **bhm,    void *buf, const std::size_t bufsize);

    private:
        /** Allocates space for a temporary message and unpacks only the header */
        static int unpack(Message              **msg,    void *buf, const std::size_t bufsize);

        /** Unpacks the message header in a preallocated space */
        static int unpack(Message              *msg,     void *buf, const std::size_t bufsize, char **curr);
};

}

#endif
